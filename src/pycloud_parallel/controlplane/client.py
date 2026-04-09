from __future__ import annotations

"""Client helpers for InfoCenter/NodeControl service-session workflow."""

import asyncio
import base64
from collections import deque
import contextlib
import errno
import hashlib
import importlib
import inspect
import json
import logging
import io
import os
import queue
import re
import secrets
import socket
import sys
import tarfile
import tempfile
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
import uuid
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Awaitable, Any, Callable, ClassVar, Dict, Iterator, List, Optional, Sequence, Set, Tuple, Union
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode, urlparse
from urllib.request import Request, urlopen

import grpc
from google.protobuf import timestamp_pb2

from pycloud_parallel.controlplane.config import (
    FILE_HASH_CHUNK_SIZE_BYTES,
    GRPC_MAX_RECEIVE_MESSAGE_LENGTH_BYTES,
    GRPC_MAX_SEND_MESSAGE_LENGTH_BYTES,
    OBJECT_CHUNK_SIZE_BYTES,
    grpc_channel_options,
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
from pycloud_parallel.controlplane.serialization import (
    INLINE_PAYLOAD_SOFT_LIMIT_BYTES,
    convert_dict_to_arrow,
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

_SERVICE_SESSION_LOCK_GUARD = threading.Lock()
_SERVICE_SESSION_LOCKED_PATHS: Set[str] = set()


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
    if module_name and module_name != "__main__":
        return module_name
    try:
        source_file = inspect.getsourcefile(func) or inspect.getfile(func)
    except Exception:
        source_file = ""
    inferred = _infer_entry_module_from_source_file(str(source_file or ""))
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

    # 优先级 1: 模块对象（自动打包整个模块）
    if module is not None:
        if not inspect.ismodule(module):
            raise ValueError("module must be a module object")

        packager = DependencyPackager()

        # 创建临时文件
        with tempfile.NamedTemporaryFile(suffix=".tar.gz", delete=False) as tmp:
            tmp_path = tmp.name

        try:
            # 打包模块和依赖
            packager.package_module(
                module_name=module.__name__,
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
            import zipfile

            paths = [Path(str(p)) for p in artifact_path if str(p)]
            if not paths:
                raise ValueError("artifact_path list is empty")

            with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp_zip:
                tmp_zip_path = tmp_zip.name

            try:
                with zipfile.ZipFile(tmp_zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
                    for path in paths:
                        if not path.exists():
                            raise FileNotFoundError(f"Path not found: {path}")
                        if path.is_file():
                            zf.write(path, path.name)
                        elif path.is_dir():
                            dirs_to_check = {path}
                            for child in path.rglob("*"):
                                if child.is_dir():
                                    dirs_to_check.add(child)

                            for d in sorted(dirs_to_check, key=lambda x: str(x)):
                                if not (d / "__init__.py").exists():
                                    init_arcname = path.name / d.relative_to(path) / "__init__.py"
                                    zf.writestr(str(init_arcname), "")

                            for file_path in path.rglob("*"):
                                if file_path.is_file():
                                    arcname = path.name / file_path.relative_to(path)
                                    zf.write(file_path, str(arcname))

                with open(tmp_zip_path, "rb") as f:
                    return f.read(), "artifact_bundle.zip"
            finally:
                try:
                    os.unlink(tmp_zip_path)
                except Exception:
                    pass

        path = Path(artifact_path)
        if not path.exists():
            raise FileNotFoundError(f"Artifact path not found: {artifact_path}")

        # 如果是单个文件，直接读取
        if path.is_file():
            with open(path, "rb") as f:
                return f.read(), path.name

        # 如果是目录，打包成 tar.gz
        if path.is_dir():
            with tempfile.NamedTemporaryFile(suffix=".tar.gz", delete=False) as tmp:
                tmp_path = tmp.name

            try:
                with tarfile.open(tmp_path, "w:gz") as tar:
                    for item in path.rglob("*"):
                        if item.is_file():
                            arcname = item.relative_to(path)
                            tar.add(item, arcname=arcname)

                with open(tmp_path, "rb") as f:
                    blob = f.read()

                filename = f"{path.name}.tar.gz"
                return blob, filename
            finally:
                try:
                    os.unlink(tmp_path)
                except Exception:
                    pass

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


def _serialize_http_call_payload(payload: Optional[Dict[str, object]], *, context: str) -> Dict[str, object]:
    serialized_payload, _, _ = serialize_inline_payload(payload or {}, context=context)
    return serialized_payload


def _validate_task_submit_items(tasks: Sequence[pb2.TaskSubmitItem], *, request_context: str) -> None:
    validate_inline_payload_structs(
        [item.payload for item in tasks],
        item_context="task payload",
        request_context=request_context,
    )


def _is_bundle_format(fmt: str, *, expected: str) -> bool:
    return str(fmt or "").strip().lower() == expected


def _materialize_downloaded_result(path: Path, *, result_ref: ResultRef):
    materialized = normalize_materialize_as(result_ref.materialize_as, default="path")
    log_payload_flow(
        "result_materialize",
        materialize_as=materialized,
        format=result_ref.format,
        path=str(path),
    )
    if materialized == "path":
        return path
    if materialized == "bytes":
        return path.read_bytes()
    if materialized == "json":
        return json.loads(path.read_text(encoding="utf-8"))
    if materialized == "ndarray":
        import numpy as np

        return np.load(path, allow_pickle=False)
    if materialized == "dataframe":
        import pandas as pd
        if _is_bundle_format(result_ref.format, expected="dfbundle"):
            import zipfile

            with zipfile.ZipFile(path) as zf:
                if {"data.parquet", "meta.json"}.issubset(set(zf.namelist())):
                    with zf.open("data.parquet") as fh:
                        frame = pd.read_parquet(fh)
                    with zf.open("meta.json") as fh:
                        meta = json.load(fh)
                    return deserialize_dataframe_bundle(meta, frame)
        return pd.read_parquet(path)
    if materialized == "series":
        import pandas as pd
        if _is_bundle_format(result_ref.format, expected="seriesbundle"):
            import zipfile

            with zipfile.ZipFile(path) as zf:
                if {"data.parquet", "meta.json"}.issubset(set(zf.namelist())):
                    with zf.open("data.parquet") as fh:
                        frame = pd.read_parquet(fh)
                    with zf.open("meta.json") as fh:
                        meta = json.load(fh)
                    if len(frame.columns) != 1:
                        raise ValueError("series bundle parquet must contain exactly one column")
                    return deserialize_series_bundle(meta, frame.iloc[:, 0])
        frame = pd.read_parquet(path)
        if len(frame.columns) != 1:
            raise ValueError("series parquet must contain exactly one column")
        return frame.iloc[:, 0]
    raise ValueError(f"unsupported result materialize_as: {result_ref.materialize_as!r}")


def _inject_result_ref_control_addr(value: object, *, control_addr: str) -> object:
    if isinstance(value, ResultRef) and control_addr and not value.control_addr:
        return replace(value, control_addr=control_addr)
    return value


def _normalize_http_response_body(body: object, *, control_addr: str = "") -> Dict[str, object]:
    if not isinstance(body, dict):
        raise RuntimeError("invalid json response")
    converted = convert_dict_to_arrow(body)
    if not isinstance(converted, dict):
        raise RuntimeError("invalid json response")
    if "data" in converted:
        converted = dict(converted)
        converted["data"] = _inject_result_ref_control_addr(converted.get("data"), control_addr=control_addr)
    return converted


def _extract_result_ref(value: object) -> Optional[ResultRef]:
    if isinstance(value, ResultRef):
        return value
    if isinstance(value, dict):
        data = value.get("data")
        if isinstance(data, ResultRef):
            return data
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
    fd, tmp_name = tempfile.mkstemp(prefix="pycloud-pkg-", suffix=".tar.gz")
    os.close(fd)
    out = Path(tmp_name)
    with tarfile.open(out, "w:gz") as tf:
        for item in sorted(dir_path.rglob("*")):
            if item.name == "__pycache__":
                continue
            rel = item.relative_to(dir_path)
            tf.add(item, arcname=str(rel))
    return out


def _package_paths_to_targz(*, root_dir: Path, paths: Sequence[str]) -> Path:
    normalized: List[Path] = []
    root = root_dir.resolve()
    for item in paths:
        p = (root / item).resolve()
        if not p.exists():
            raise FileNotFoundError(f"path not found: {item}")
        if p != root and root not in p.parents:
            raise ValueError(f"path escapes root_dir: {item}")
        normalized.append(p)

    fd, tmp_name = tempfile.mkstemp(prefix="pycloud-paths-", suffix=".tar.gz")
    os.close(fd)
    out = Path(tmp_name)
    with tarfile.open(out, "w:gz") as tf:
        for p in normalized:
            rel = p.relative_to(root)
            tf.add(p, arcname=str(rel))
    return out


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
        return "json", normalize_object_format(format or "json", default="json"), json.dumps(data, ensure_ascii=False).encode("utf-8")

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
    serialized = serialize_arrow_compatible(value)
    return len(json.dumps(serialized, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))


def _prepare_managed_global_value_for_upload(
    clients: Sequence["NodeControlClient"],
    value: Any,
    *,
    object_threshold_bytes: int = INLINE_PAYLOAD_SOFT_LIMIT_BYTES,
) -> Any:
    if isinstance(value, ObjectRef):
        return value
    if isinstance(value, os.PathLike):
        return _put_data_via_clients(clients, value)
    if isinstance(value, str):
        path = Path(value).expanduser()
        if path.exists() and path.is_file():
            return _put_data_via_clients(clients, path)
        return value
    if isinstance(value, (bytes, bytearray, memoryview)):
        return _put_data_via_clients(clients, bytes(value), format="bin")

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
        if isinstance(value, (dict, list)):
            prepared = _put_data_via_clients(clients, value, format="json")
        else:
            prepared = _put_data_via_clients(clients, value)
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


_SERVICE_SESSION_SCHEMA_VERSION = 2


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


def _service_session_cache_file(
    *,
    owner_client_id: str,
    service_name: str,
    cache_dir: str = "",
) -> Path:
    base_dir = Path(cache_dir).expanduser() if str(cache_dir).strip() else _default_service_session_cache_dir()
    return (
        base_dir
        / _sanitize_session_cache_part(owner_client_id)
        / f"{_sanitize_session_cache_part(service_name)}.json"
    )


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


@dataclass
class _ServiceSessionFileLock:
    path: Path
    _fp: Optional[io.BufferedRandom] = field(default=None, init=False, repr=False)

    def acquire(self) -> "_ServiceSessionFileLock":
        normalized = str(self.path.resolve())
        with _SERVICE_SESSION_LOCK_GUARD:
            if normalized in _SERVICE_SESSION_LOCKED_PATHS:
                raise RuntimeError(f"local deploy session already holds cache lock: {self.path}")
            _SERVICE_SESSION_LOCKED_PATHS.add(normalized)
        try:
            _ensure_private_dir(self.path.parent)
            fp = open(self.path, "a+b")
        except Exception:
            with _SERVICE_SESSION_LOCK_GUARD:
                _SERVICE_SESSION_LOCKED_PATHS.discard(normalized)
            raise
        try:
            try:
                os.chmod(self.path, 0o600)
            except OSError:
                pass
            if os.name == "nt":
                import msvcrt

                fp.seek(0)
                msvcrt.locking(fp.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(fp.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except Exception as exc:
            fp.close()
            with _SERVICE_SESSION_LOCK_GUARD:
                _SERVICE_SESSION_LOCKED_PATHS.discard(normalized)
            raise RuntimeError(
                f"another local deploy process already owns service session cache lock: {self.path}"
            ) from exc
        self._fp = fp
        return self

    def write_json(self, payload: Dict[str, object]) -> None:
        if self._fp is None:
            raise RuntimeError("service session lock is not acquired")
        data = json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True).encode("utf-8") + b"\n"
        self._fp.seek(0)
        self._fp.truncate()
        self._fp.write(data)
        self._fp.flush()
        try:
            os.fsync(self._fp.fileno())
        except OSError:
            pass

    def clear(self) -> None:
        if self._fp is None:
            return
        self._fp.seek(0)
        self._fp.truncate()
        self._fp.flush()
        try:
            os.fsync(self._fp.fileno())
        except OSError:
            pass

    def close(self) -> None:
        if self._fp is None:
            return
        normalized = str(self.path.resolve())
        try:
            if os.name == "nt":
                import msvcrt

                self._fp.seek(0)
                msvcrt.locking(self._fp.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(self._fp.fileno(), fcntl.LOCK_UN)
        finally:
            try:
                self._fp.close()
            finally:
                self._fp = None
                with _SERVICE_SESSION_LOCK_GUARD:
                    _SERVICE_SESSION_LOCKED_PATHS.discard(normalized)


def _load_service_session_cache(
    *,
    owner_client_id: str,
    service_name: str,
    cache_dir: str = "",
) -> Optional[Dict[str, object]]:
    path = _service_session_cache_file(
        owner_client_id=owner_client_id,
        service_name=service_name,
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
    if int(payload.get("schema_version", 0) or 0) != _SERVICE_SESSION_SCHEMA_VERSION:
        return None
    if payload.get("owner_client_id") != owner_client_id or payload.get("service_name") != service_name:
        return None
    nodes = payload.get("nodes")
    if not isinstance(nodes, dict):
        return None
    return payload


@dataclass(frozen=True)
class InfoCenterNodeService:
    service_name: str
    service_id: str
    status: int
    status_text: str = ""
    worker_count: int = 0
    alive_workers: int = 0
    in_flight: int = 0
    http_base_url: str = ""


@dataclass(frozen=True)
class InfoCenterNode:
    node_instance_id: str
    node_id: str
    control_addr: str
    healthy: bool
    capacity: int
    queue_capacity: int
    queued: int
    inflight: int
    credit: int
    python_version: str = ""
    active_runtimes: Tuple[str, ...] = ()
    tags: Tuple[str, ...] = ()
    service_worker_capacity: int = 0
    service_worker_used: int = 0
    service_worker_available: int = 0
    schedulable: bool = True
    drain: bool = False
    reason: str = ""
    loaded_services: Tuple[str, ...] = ()
    services: Tuple[InfoCenterNodeService, ...] = ()


@dataclass(frozen=True)
class InfoCenterServiceRoute:
    service_name: str
    service_id: str
    status: int
    node_instance_id: str
    node_id: str
    control_addr: str
    node_healthy: bool
    worker_count: int
    alive_workers: int
    in_flight: int
    lease_expire_at: datetime
    http_base_url: str


@dataclass
class NodeCircuitState:
    state: str = "closed"  # closed | open | half_open
    consecutive_failures: int = 0
    open_until_monotonic: float = 0.0
    open_count: int = 0
    probe_in_flight: bool = False
    last_error: str = ""


def _node_instance_key_from_node(node: InfoCenterNode) -> str:
    return str(getattr(node, "node_instance_id", "") or getattr(node, "node_id", "") or getattr(node, "control_addr", "")).strip()


def _node_instance_key_from_route(route: InfoCenterServiceRoute) -> str:
    return str(getattr(route, "node_instance_id", "") or getattr(route, "node_id", "") or getattr(route, "control_addr", "")).strip()


def _build_unique_node_id_map(nodes: Sequence[InfoCenterNode], *, requested_ids: Optional[Sequence[str]] = None) -> Dict[str, InfoCenterNode]:
    out: Dict[str, InfoCenterNode] = {}
    duplicates: set[str] = set()
    for node in nodes:
        node_id = str(getattr(node, "node_id", "") or "").strip()
        if not node_id:
            continue
        if node_id in out:
            duplicates.add(node_id)
            continue
        out[node_id] = node
    relevant_duplicates = duplicates if requested_ids is None else (duplicates & {str(x).strip() for x in requested_ids if str(x).strip()})
    if relevant_duplicates:
        dup_list = sorted(relevant_duplicates)
        raise RuntimeError(
            f"requested node_ids are ambiguous because multiple live node instances share the same node_id: {dup_list}; "
            "please select by node_instance_ids instead"
        )
    return out


@dataclass
class _RouteLocalState:
    consecutive_failures: int = 0
    open_until_monotonic: float = 0.0
    last_error: str = ""


@dataclass
class _ServiceRouteSnapshot:
    service_name: str
    routes: List[InfoCenterServiceRoute] = field(default_factory=list)
    refreshed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


class InfoCenterClient:
    """Thin HTTP + JSON client wrapper for InfoCenter service."""

    def __init__(self, target: str, *, timeout_sec: float = 10.0) -> None:
        self.target = target
        self.base_url = _target_to_base_url(target)
        self.timeout_sec = max(0.1, float(timeout_sec))

    def close(self) -> None:
        return None

    def __enter__(self) -> "InfoCenterClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def register_node(
        self,
        *,
        node_id: str,
        node_instance_id: str = "",
        control_addr: str,
        capacity: int = 32,
        queue_capacity: int = 4000,
        tags: Optional[Sequence[str]] = None,
        version: str = "",
        metadata: Optional[Dict[str, str]] = None,
        services: Optional[Sequence[pb2.ServiceRouteReport]] = None,
        active_runtimes: Optional[Sequence[str]] = None,
        service_worker_capacity: int = 0,
        service_worker_used: int = 0,
        python_version: str = "",
    ) -> Dict[str, object]:
        serialized_services = []
        for item in services or []:
            serialized_services.append(
                {
                    "service_name": str(item.service_name),
                    "service_id": str(item.service_id),
                    "status": int(item.status),
                    "worker_count": int(item.worker_count),
                    "alive_workers": int(item.alive_workers),
                    "in_flight": int(item.in_flight),
                    "http_base_url": str(item.http_base_url),
                }
            )
        return _http_json_request(
            base_url=self.base_url,
            path="/nodes/register",
            method="POST",
            timeout_sec=self.timeout_sec,
            payload={
                "node_id": node_id,
                "node_instance_id": str(node_instance_id or "").strip(),
                "control_addr": control_addr,
                "capacity": max(1, int(capacity)),
                "queue_capacity": max(1, int(queue_capacity)),
                "tags": list(tags or []),
                "version": version,
                "metadata": dict(metadata or {}),
                "services": serialized_services,
                "python_version": str(python_version or "").strip(),
                "active_runtimes": [str(x).strip() for x in (active_runtimes or []) if str(x).strip()],
                "service_worker_capacity": max(0, int(service_worker_capacity or 0)),
                "service_worker_used": max(0, int(service_worker_used or 0)),
            },
        )

    def heartbeat_node(
        self,
        *,
        node_id: str,
        node_instance_id: str = "",
        healthy: bool = True,
        metrics: Optional[Dict[str, object]] = None,
        metadata: Optional[Dict[str, str]] = None,
        services: Optional[Sequence[pb2.ServiceRouteReport]] = None,
        active_runtimes: Optional[Sequence[str]] = None,
        service_worker_capacity: int = 0,
        service_worker_used: int = 0,
        python_version: str = "",
    ) -> Dict[str, object]:
        serialized_services = []
        for item in services or []:
            serialized_services.append(
                {
                    "service_name": str(item.service_name),
                    "service_id": str(item.service_id),
                    "status": int(item.status),
                    "worker_count": int(item.worker_count),
                    "alive_workers": int(item.alive_workers),
                    "in_flight": int(item.in_flight),
                    "http_base_url": str(item.http_base_url),
                }
            )
        return _http_json_request(
            base_url=self.base_url,
            path="/nodes/heartbeat",
            method="POST",
            timeout_sec=self.timeout_sec,
            payload={
                "node_id": node_id,
                "node_instance_id": str(node_instance_id or "").strip(),
                "healthy": bool(healthy),
                "metrics": dict(metrics or {}),
                "metadata": dict(metadata or {}),
                "services": serialized_services,
                "python_version": str(python_version or "").strip(),
                "active_runtimes": [str(x).strip() for x in (active_runtimes or []) if str(x).strip()],
                "service_worker_capacity": max(0, int(service_worker_capacity or 0)),
                "service_worker_used": max(0, int(service_worker_used or 0)),
            },
        )

    def list_nodes(
        self,
        *,
        healthy_only: bool = True,
        tags: Optional[Sequence[str]] = None,
        limit: int = 100,
    ) -> Sequence[InfoCenterNode]:
        params = urlencode(
            {
                "healthy_only": "true" if healthy_only else "false",
                "tags": ",".join([x for x in (tags or []) if x]),
                "limit": str(max(1, int(limit))),
            }
        )
        resp = _http_json_request(
            base_url=self.base_url,
            path=f"/nodes?{params}",
            method="GET",
            timeout_sec=self.timeout_sec,
        )
        out = []
        for item in resp.get("nodes", []):
            services = []
            for svc in item.get("services", []) or []:
                services.append(
                    InfoCenterNodeService(
                        service_name=str(svc.get("service_name", "") or ""),
                        service_id=str(svc.get("service_id", "") or ""),
                        status=int(svc.get("status", 0) or 0),
                        status_text=str(svc.get("status_text", "") or ""),
                        worker_count=int(svc.get("worker_count", 0) or 0),
                        alive_workers=int(svc.get("alive_workers", 0) or 0),
                        in_flight=int(svc.get("in_flight", 0) or 0),
                        http_base_url=str(svc.get("http_base_url", "") or ""),
                    )
                )
            out.append(
                InfoCenterNode(
                    node_instance_id=str(item.get("node_instance_id", "") or item.get("node_id", "") or ""),
                    node_id=str(item.get("node_id", "")),
                    control_addr=str(item.get("control_addr", "")),
                    healthy=bool(item.get("healthy", False)),
                    capacity=int(item.get("capacity", 0) or 0),
                    queue_capacity=int(item.get("queue_capacity", 0) or 0),
                    queued=int(item.get("queued", 0) or 0),
                    inflight=int(item.get("inflight", 0) or 0),
                    credit=int(item.get("credit", 0) or 0),
                    python_version=str(item.get("python_version", "") or ""),
                    active_runtimes=tuple(item.get("active_runtimes") or ()),
                    tags=tuple(item.get("tags") or ()),
                    service_worker_capacity=int(item.get("service_worker_capacity", 0) or 0),
                    service_worker_used=int(item.get("service_worker_used", 0) or 0),
                    service_worker_available=int(item.get("service_worker_available", 0) or 0),
                    schedulable=bool(item.get("schedulable", True)),
                    drain=bool(item.get("drain", False)),
                    reason=str(item.get("reason", "") or ""),
                    loaded_services=tuple(item.get("loaded_services") or ()),
                    services=tuple(services),
                )
            )
        return out

    def list_service_routes(
        self,
        *,
        service_name: str = "",
        healthy_only: bool = True,
        limit: int = 500,
    ) -> Sequence[InfoCenterServiceRoute]:
        params = urlencode(
            {
                "service_name": service_name,
                "healthy_only": "true" if healthy_only else "false",
                "limit": str(max(1, int(limit))),
            }
        )
        resp = _http_json_request(
            base_url=self.base_url,
            path=f"/services/routes?{params}",
            method="GET",
            timeout_sec=self.timeout_sec,
        )
        out = []
        for item in resp.get("routes", []):
            dt_text = str(item.get("lease_expire_at", "") or "")
            dt = datetime.fromisoformat(dt_text) if dt_text else datetime.now(timezone.utc)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            out.append(
                InfoCenterServiceRoute(
                    service_name=str(item.get("service_name", "")),
                    service_id=str(item.get("service_id", "")),
                    status=int(item.get("status", 0) or 0),
                    node_instance_id=str(item.get("node_instance_id", "") or item.get("node_id", "") or ""),
                    node_id=str(item.get("node_id", "")),
                    control_addr=str(item.get("control_addr", "")),
                    node_healthy=bool(item.get("node_healthy", False)),
                    worker_count=int(item.get("worker_count", 0) or 0),
                    alive_workers=int(item.get("alive_workers", 0) or 0),
                    in_flight=int(item.get("in_flight", 0) or 0),
                    lease_expire_at=dt.astimezone(timezone.utc),
                    http_base_url=str(item.get("http_base_url", "")),
                )
            )
        return out

    def select_task_nodes(
        self,
        *,
        healthy_only: bool = True,
        tags: Optional[Sequence[str]] = None,
        node_ids: Optional[Sequence[str]] = None,
        node_instance_ids: Optional[Sequence[str]] = None,
        node_count: int = 0,
        limit: int = 100,
        require_credit: bool = True,
        preferred_runtime_key: str = "",
        runtime: str = "",
    ) -> Sequence[InfoCenterNode]:
        nodes = list(self.list_nodes(healthy_only=healthy_only, tags=tags, limit=limit))
        requested_node_ids = [str(node_id).strip() for node_id in (node_ids or []) if str(node_id).strip()]
        requested_instance_ids = [str(node_id).strip() for node_id in (node_instance_ids or []) if str(node_id).strip()]
        preferred_runtime = str(preferred_runtime_key or "").strip()
        normalized_runtime = normalize_python_runtime_spec(runtime)
        discovered_instance_map = {_node_instance_key_from_node(node): node for node in nodes}

        if requested_instance_ids:
            missing_instance_ids = [node_id for node_id in requested_instance_ids if node_id not in discovered_instance_map]
            if missing_instance_ids:
                raise RuntimeError(f"requested node_instance_ids not found in current discovery scope: {missing_instance_ids}")
            selected = [discovered_instance_map[node_id] for node_id in requested_instance_ids]
            if normalized_runtime:
                incompatible = [
                    node.node_instance_id
                    for node in selected
                    if str(node.python_version or "").strip()
                    and not matches_python_runtime(node.python_version, normalized_runtime)
                ]
                if incompatible:
                    raise RuntimeError(
                        f"requested node_instance_ids do not satisfy runtime {normalized_runtime}: {incompatible}"
                    )
        elif requested_node_ids:
            discovered_node_map = _build_unique_node_id_map(nodes, requested_ids=requested_node_ids)
            missing_node_ids = [node_id for node_id in requested_node_ids if node_id not in discovered_node_map]
            if missing_node_ids:
                raise RuntimeError(f"requested node_ids not found in current discovery scope: {missing_node_ids}")
            selected = [discovered_node_map[node_id] for node_id in requested_node_ids]
            if normalized_runtime:
                incompatible = [
                    node.node_id
                    for node in selected
                    if str(node.python_version or "").strip()
                    and not matches_python_runtime(node.python_version, normalized_runtime)
                ]
                if incompatible:
                    raise RuntimeError(
                        f"requested node_ids do not satisfy runtime {normalized_runtime}: {incompatible}"
                    )
        else:
            candidates = [
                node
                for node in nodes
                if node.healthy and node.schedulable and not node.drain and (not require_credit or node.credit > 0)
            ]
            if normalized_runtime:
                candidates = _filter_nodes_by_runtime(candidates, runtime=normalized_runtime)
            if not candidates:
                if normalized_runtime:
                    raise RuntimeError(
                        f"no schedulable task nodes from InfoCenter for runtime {normalized_runtime}"
                    )
                raise RuntimeError("no schedulable task nodes from InfoCenter")
            candidates.sort(
                key=lambda node: (
                    0 if preferred_runtime and preferred_runtime in node.active_runtimes else 1,
                    -int(node.credit),
                    int(node.queued),
                    int(node.inflight),
                    node.node_id,
                )
            )
            requested_count = int(node_count or 0)
            selected = candidates if requested_count <= 0 else candidates[:requested_count]

        if not selected:
            raise RuntimeError("no task nodes selected from InfoCenter")
        return selected


class GatewayServiceClient:
    """Thin HTTP + JSON client wrapper for ControlPlane Gateway service calls."""

    def __init__(self, target: str, *, timeout_sec: float = 10.0, service_token: str = "") -> None:
        self.target = target
        self.base_url = _target_to_base_url(target)
        self.timeout_sec = max(0.1, float(timeout_sec))
        self.service_token = str(service_token or "").strip()

    def close(self) -> None:
        return None

    def __enter__(self) -> "GatewayServiceClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def call(
        self,
        *,
        service_name: str,
        method: str,
        payload: Optional[Dict[str, object]] = None,
        timeout_sec: float = 60.0,
        service_token: Optional[str] = None,
    ) -> Dict[str, object]:
        name = str(service_name or "").strip()
        method_name = str(method or "").strip()
        if not name:
            raise ValueError("service_name is required")
        if not method_name:
            raise ValueError("method is required")
        token = self.service_token if service_token is None else str(service_token or "").strip()
        headers: Dict[str, str] = {}
        if token:
            headers["X-Service-Token"] = token
        params = urlencode({"timeout_sec": f"{max(0.1, float(timeout_sec)):.3f}"})
        serialized_payload = _serialize_http_call_payload(payload, context="service call payload")
        return _http_json_request(
            base_url=self.base_url,
            path=f"/svc/{quote(name, safe='')}/call/{quote(method_name, safe='')}?{params}",
            method="POST",
            timeout_sec=max(self.timeout_sec, max(0.1, float(timeout_sec)) + 1.0),
            payload=serialized_payload,
            headers=headers,
        )

    def list_methods(self, *, service_name: str, include_docs: bool = False) -> Sequence[Dict[str, object]]:
        name = str(service_name or "").strip()
        if not name:
            raise ValueError("service_name is required")
        params = urlencode({"include_docs": "true" if include_docs else "false"})
        resp = _http_json_request(
            base_url=self.base_url,
            path=f"/svc/{quote(name, safe='')}/methods?{params}",
            method="GET",
            timeout_sec=self.timeout_sec,
        )
        methods = resp.get("methods", [])
        if not isinstance(methods, list):
            raise RuntimeError("invalid methods response")
        return [item for item in methods if isinstance(item, dict)]

    def get_status(self, *, service_name: str) -> Dict[str, object]:
        name = str(service_name or "").strip()
        if not name:
            raise ValueError("service_name is required")
        return _http_json_request(
            base_url=self.base_url,
            path=f"/svc/{quote(name, safe='')}/status",
            method="GET",
            timeout_sec=self.timeout_sec,
        )

    def download_result_to_file(self, response_or_data: object, *, target_path: str) -> Path:
        ref = _extract_result_ref(response_or_data)
        if ref is None:
            raise ValueError("service result is inline data; no download needed")
        if not ref.control_addr:
            raise RuntimeError("service result is missing control_addr for download")
        with NodeControlClient(ref.control_addr, timeout_sec=self.timeout_sec) as client:
            return client.download_result_to_file(ref, target_path=target_path)

    def fetch_result_data(self, response_or_data: object, *, target_path: str = ""):
        ref = _extract_result_ref(response_or_data)
        if ref is None:
            if isinstance(response_or_data, dict) and "data" in response_or_data:
                return response_or_data["data"]
            return response_or_data
        if not ref.control_addr:
            raise RuntimeError("service result is missing control_addr for download")
        with NodeControlClient(ref.control_addr, timeout_sec=self.timeout_sec) as client:
            return client.fetch_result_ref_data(ref, target_path=target_path)


class JobQueueClient:
    """Thin HTTP client for controlplane job queue endpoints."""

    def __init__(self, target: str, *, timeout_sec: float = 10.0) -> None:
        self.target = target
        self.base_url = _target_to_base_url(target)
        self.timeout_sec = max(0.1, float(timeout_sec))

    def close(self) -> None:
        return None

    def __enter__(self) -> "JobQueueClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def submit_job(self, payload: Dict[str, object]) -> Dict[str, object]:
        return _http_json_request(
            base_url=self.base_url,
            path="/jobs/submit",
            method="POST",
            timeout_sec=self.timeout_sec,
            payload=payload,
        )

    def get_job_status(self, job_id: str) -> Dict[str, object]:
        normalized = str(job_id or "").strip()
        if not normalized:
            raise ValueError("job_id is required")
        return _http_json_request(
            base_url=self.base_url,
            path=f"/jobs/{quote(normalized, safe='')}",
            method="GET",
            timeout_sec=self.timeout_sec,
        )

    def cancel_job(self, job_id: str) -> Dict[str, object]:
        normalized = str(job_id or "").strip()
        if not normalized:
            raise ValueError("job_id is required")
        return _http_json_request(
            base_url=self.base_url,
            path=f"/jobs/{quote(normalized, safe='')}/cancel",
            method="POST",
            timeout_sec=self.timeout_sec,
            payload={},
        )

    def submit_job_from_bytes(
        self,
        *,
        blob: bytes,
        driver_entry_module: str,
        driver_entry_callable: str = "run",
        driver_payload: Optional[Dict[str, object]] = None,
        driver_package_format: str = "py",
        priority: int = 0,
        client_id: str = "",
        runtime: str = "py3",
        task_blob: Optional[bytes] = None,
        task_entry_module: str = "",
        task_entry_callable: str = "run",
        task_package_format: str = "py",
        tags: Optional[Sequence[str]] = None,
        node_count: int = 0,
        pool_name: str = "",
        pool_worker_count: int = 1,
        pool_node_count: int = 1,
        pool_heartbeat_timeout_sec: int = 30,
        pool_idle_ttl_sec: int = 0,
        pool_allow_partial: bool = True,
        pool_min_success_nodes: int = 1,
        wait_timeout_sec: float = 3600.0,
        task_priority: int = 1,
        dependency_allowlist: Optional[Sequence[str]] = None,
    ) -> Dict[str, object]:
        payload: Dict[str, object] = {
            "client_id": str(client_id or "").strip(),
            "priority": max(0, int(priority)),
            "runtime": str(runtime or "py3"),
            "blob_b64": base64.b64encode((task_blob if task_blob is not None else blob)).decode("utf-8"),
            "entry_module": str(task_entry_module or "").strip(),
            "entry_callable": str(task_entry_callable or "run").strip() or "run",
            "package_format": str(task_package_format or "py").strip() or "py",
            "tags": list(tags or ()),
            "node_count": int(node_count or 0),
            "wait_timeout_sec": float(wait_timeout_sec or 3600.0),
            "task_priority": max(1, int(task_priority or 1)),
            "dependency_allowlist": list(dependency_allowlist or ()),
            "pool_name": str(pool_name or "").strip(),
            "pool_worker_count": max(1, int(pool_worker_count or 1)),
            "pool_node_count": max(1, int(pool_node_count or 1)),
            "pool_heartbeat_timeout_sec": max(5, int(pool_heartbeat_timeout_sec or 30)),
            "pool_idle_ttl_sec": max(0, int(pool_idle_ttl_sec or 0)),
            "pool_allow_partial": bool(pool_allow_partial),
            "pool_min_success_nodes": max(1, int(pool_min_success_nodes or 1)),
            "driver_blob_b64": base64.b64encode(blob).decode("utf-8"),
            "driver_entry_module": str(driver_entry_module or "").strip(),
            "driver_entry_callable": str(driver_entry_callable or "run").strip() or "run",
            "driver_payload": dict(driver_payload or {}),
            "driver_package_format": str(driver_package_format or "py").strip() or "py",
        }
        return self.submit_job(payload)

    def submit_job_from_func(
        self,
        *,
        func: Callable,
        driver_entry_callable: Optional[str] = None,
        driver_payload: Optional[Dict[str, object]] = None,
        priority: int = 0,
        client_id: str = "",
        runtime: str = "py3",
        task_func: Optional[Callable] = None,
        task_entry_callable: str = "run",
        tags: Optional[Sequence[str]] = None,
        node_count: int = 0,
        pool_name: str = "",
        pool_worker_count: int = 1,
        pool_node_count: int = 1,
        pool_heartbeat_timeout_sec: int = 30,
        pool_idle_ttl_sec: int = 0,
        pool_allow_partial: bool = True,
        pool_min_success_nodes: int = 1,
        wait_timeout_sec: float = 3600.0,
        task_priority: int = 1,
        dependency_allowlist: Optional[Sequence[str]] = None,
    ) -> Dict[str, object]:
        driver_blob, _ = _prepare_code_blob(func=func)
        driver_module = _default_entry_module_for_func(func)
        actual_driver_callable = str(driver_entry_callable or func.__name__).strip() or func.__name__
        effective_task_func = task_func or func
        task_blob, _ = _prepare_code_blob(func=effective_task_func)
        task_module = _default_entry_module_for_func(effective_task_func)
        actual_task_callable = str(task_entry_callable or effective_task_func.__name__).strip() or effective_task_func.__name__
        return self.submit_job_from_bytes(
            blob=driver_blob or b"",
            driver_entry_module=driver_module,
            driver_entry_callable=actual_driver_callable,
            driver_payload=driver_payload,
            priority=priority,
            client_id=client_id,
            runtime=runtime,
            task_blob=task_blob,
            task_entry_module=task_module,
            task_entry_callable=actual_task_callable,
            tags=tags,
            node_count=node_count,
            pool_name=pool_name,
            pool_worker_count=pool_worker_count,
            pool_node_count=pool_node_count,
            pool_heartbeat_timeout_sec=pool_heartbeat_timeout_sec,
            pool_idle_ttl_sec=pool_idle_ttl_sec,
            pool_allow_partial=pool_allow_partial,
            pool_min_success_nodes=pool_min_success_nodes,
            wait_timeout_sec=wait_timeout_sec,
            task_priority=task_priority,
            dependency_allowlist=dependency_allowlist,
        )

    def submit_job_from_module(
        self,
        *,
        module: Any,
        driver_entry_callable: str = "run",
        driver_payload: Optional[Dict[str, object]] = None,
        priority: int = 0,
        client_id: str = "",
        runtime: str = "py3",
        task_module: Optional[Any] = None,
        task_entry_callable: str = "run",
        tags: Optional[Sequence[str]] = None,
        node_count: int = 0,
        pool_name: str = "",
        pool_worker_count: int = 1,
        pool_node_count: int = 1,
        pool_heartbeat_timeout_sec: int = 30,
        pool_idle_ttl_sec: int = 0,
        pool_allow_partial: bool = True,
        pool_min_success_nodes: int = 1,
        wait_timeout_sec: float = 3600.0,
        task_priority: int = 1,
        dependency_allowlist: Optional[Sequence[str]] = None,
    ) -> Dict[str, object]:
        driver_blob, _ = _prepare_code_blob(module=module)
        driver_module_name = _default_entry_module_for_module(module)
        effective_task_module = task_module or module
        task_blob, _ = _prepare_code_blob(module=effective_task_module)
        task_module_name = _default_entry_module_for_module(effective_task_module)
        return self.submit_job_from_bytes(
            blob=driver_blob or b"",
            driver_entry_module=driver_module_name,
            driver_entry_callable=driver_entry_callable,
            driver_payload=driver_payload,
            priority=priority,
            client_id=client_id,
            runtime=runtime,
            task_blob=task_blob,
            task_entry_module=task_module_name,
            task_entry_callable=task_entry_callable,
            tags=tags,
            node_count=node_count,
            pool_name=pool_name,
            pool_worker_count=pool_worker_count,
            pool_node_count=pool_node_count,
            pool_heartbeat_timeout_sec=pool_heartbeat_timeout_sec,
            pool_idle_ttl_sec=pool_idle_ttl_sec,
            pool_allow_partial=pool_allow_partial,
            pool_min_success_nodes=pool_min_success_nodes,
            wait_timeout_sec=wait_timeout_sec,
            task_priority=task_priority,
            dependency_allowlist=dependency_allowlist,
        )

    def wait_for_terminal(
        self,
        job_id: str,
        *,
        timeout_sec: float = 30.0,
        poll_interval_sec: float = 0.5,
    ) -> Dict[str, object]:
        normalized = str(job_id or "").strip()
        if not normalized:
            raise ValueError("job_id is required")
        deadline = time.time() + max(0.1, float(timeout_sec))
        while time.time() < deadline:
            payload = self.get_job_status(normalized)
            job = dict(payload.get("job") or {})
            status = str(job.get("status", "") or "")
            if status in {"SUCCEEDED", "FAILED", "CANCELLED"}:
                return payload
            time.sleep(max(0.05, float(poll_interval_sec)))
        raise TimeoutError(f"job did not reach terminal state before timeout: {normalized}")


@dataclass
class NativeTaskPoolClient:
    _client: "NodeControlClient" = field(repr=False)
    owner_client_id: str
    pool_id: str
    pool_token: str
    code_version: str
    worker_count: int
    heartbeat_timeout_sec: int = 30

    def submit_tasks(
        self,
        tasks: Sequence[pb2.TaskSubmitItem],
        *,
        job_id: str = "",
    ) -> pb2.SubmitTasksResponse:
        return self._client.submit_pool_tasks(
            pool_id=self.pool_id,
            pool_token=self.pool_token,
            tasks=tasks,
            job_id=job_id,
        )

    def pull_results(
        self,
        *,
        limit: int = 100,
        wait_ms: int = 0,
        cursor: str = "",
    ) -> pb2.PullResultsResponse:
        return self._client.pull_pool_results(
            pool_id=self.pool_id,
            pool_token=self.pool_token,
            limit=limit,
            wait_ms=wait_ms,
            cursor=cursor,
        )

    def close(self, *, reason: str = "") -> pb2.CloseTaskPoolResponse:
        return self._client.close_task_pool(
            owner_client_id=self.owner_client_id,
            pool_id=self.pool_id,
            pool_token=self.pool_token,
            reason=reason,
        )

    def heartbeat(self, *, seq: int = 0) -> pb2.HeartbeatTaskPoolResponse:
        return self._client.heartbeat_task_pool(
            owner_client_id=self.owner_client_id,
            pool_id=self.pool_id,
            pool_token=self.pool_token,
            seq=seq,
        )

    def cancel_job(self, *, job_id: str, reason: str = "") -> pb2.CancelJobResponse:
        return self._client.cancel_pool_job(
            pool_id=self.pool_id,
            pool_token=self.pool_token,
            job_id=job_id,
            reason=reason,
        )

    def get_status(self) -> pb2.TaskPoolStatusInfo:
        return self._client.get_task_pool_status(pool_id=self.pool_id, pool_token=self.pool_token)


@dataclass
class _TaskPoolCallProxy:
    session: Any
    method_name: str

    def _build_payload(self, *args, **kwargs) -> Dict[str, object]:
        payload: Dict[str, object] = {}
        if args:
            payload["args"] = list(args)
            if kwargs:
                payload["kwargs"] = kwargs
        elif kwargs:
            payload.update(kwargs)
        return payload

    def submit(self, *args, **kwargs) -> str:
        payload = self._build_payload(*args, **kwargs)
        resp = self.session.submit_payloads([payload], task_method=self.method_name)
        if len(resp.accepted) != 1:
            raise RuntimeError(
                f"expected exactly one accepted task for method={self.method_name}, "
                f"got accepted={len(resp.accepted)} rejected={len(resp.rejected)}"
            )
        return str(resp.accepted[0].task_id)

    def __call__(self, *args, **kwargs) -> str:
        return self.submit(*args, **kwargs)

    def sync(self, *args, **kwargs):
        enter_exclusive = getattr(self.session, "_enter_exclusive_mode", None)
        exit_exclusive = getattr(self.session, "_exit_exclusive_mode", None)
        entered_exclusive = False
        if callable(enter_exclusive) and callable(exit_exclusive):
            enter_exclusive("run.sync", require_clean=True)
            entered_exclusive = True
        try:
            task_id = self.submit(*args, **kwargs)
            items = self.session._collect_data_for_task_ids({task_id}, timeout_sec=30.0)  # noqa: SLF001
            results = [data for _, data in items]
            if len(results) == 1:
                return results[0]
            return results
        finally:
            if entered_exclusive:
                exit_exclusive("run.sync")


class DedicatedTaskServiceSession:
    """Dedicated temporary task pool backed by a hidden ServiceGroup."""

    def __init__(
        self,
        *,
        group: "ServiceGroup",
        task_method: str,
        job_id: str = "",
        max_submit_workers: int = 0,
    ) -> None:
        self._group = group
        self._task_method = str(task_method or "run").strip() or "run"
        self._job_id = str(job_id or f"pool-{self._group.service_name}").strip() or f"pool-{self._group.service_name}"
        self._closed = False
        self._submit_seq = 0
        self._submit_lock = threading.Lock()
        self._results: "queue.Queue[pb2.TaskResult]" = queue.Queue()
        self._buffered_results: "deque[pb2.TaskResult]" = deque()
        self._buffer_lock = threading.Lock()
        self._futures: Dict[str, Future] = {}
        self._future_lock = threading.Lock()
        submit_workers = max(1, int(max_submit_workers or sum(int(session.worker_count or 1) for session in group.sessions.values()) or 1))
        self._executor = ThreadPoolExecutor(max_workers=submit_workers, thread_name_prefix="task-pool-submit")

    @property
    def client_id(self) -> str:
        return str(self._group.owner_client_id)

    @property
    def job_id(self) -> str:
        return self._job_id

    @property
    def code_version(self) -> str:
        return str(self._group._artifact_code_version or "")  # noqa: SLF001

    @property
    def node_ids(self) -> Sequence[str]:
        return self._group.node_ids()

    @property
    def node_instance_ids(self) -> Sequence[str]:
        return self._group.node_instance_ids()

    @property
    def methods(self) -> List[str]:
        return [self._task_method]

    def _ensure_method(self, method_name: str) -> str:
        normalized = str(method_name or "").strip()
        if not normalized:
            raise ValueError("method is required")
        if normalized != self._task_method:
            raise AttributeError(
                f"'{type(self).__name__}' has no method '{normalized}'. Available methods: {self.methods}"
            )
        return normalized

    def _next_task_id(self) -> str:
        with self._submit_lock:
            self._submit_seq += 1
            return f"{self.job_id}-task-{self._submit_seq:04d}"

    def _submit_one(self, *, task_id: str, payload: Dict[str, object], method_name: str, timeout_sec: float) -> None:
        started_at = _now_timestamp()
        try:
            _, resp = self._group.call_balanced(method_name, payload, timeout_sec=timeout_sec)
            result = pb2.TaskResult(
                task_id=task_id,
                job_id=self.job_id,
                status=pb2.TASK_STATUS_SUCCEEDED,
                attempt=1,
                started_at=started_at,
                finished_at=_now_timestamp(),
                result=dict_to_struct(resp.get("data") if isinstance(resp, dict) and "data" in resp else resp or {}),
            )
        except Exception as exc:
            result = pb2.TaskResult(
                task_id=task_id,
                job_id=self.job_id,
                status=pb2.TASK_STATUS_FAILED_INFRA,
                attempt=1,
                started_at=started_at,
                finished_at=_now_timestamp(),
                error=pb2.TaskError(type="TaskPoolError", message=str(exc)),
            )
        self._results.put(result)

    def submit_payloads(
        self,
        payloads: Sequence[Dict[str, object]],
        *,
        task_method: str = "",
        timeout_sec: float = 60.0,
        job_id: str = "",
        task_id_prefix: str = "",
        timeout_hint_sec: int = 0,
        priority: int = 1,
        runtime_key: str = "",
    ) -> pb2.SubmitTasksResponse:
        del job_id, timeout_hint_sec, priority, runtime_key
        if self._closed:
            raise RuntimeError("task pool session is closed")
        method_name = str(task_method or self._task_method).strip() or self._task_method
        accepted: List[pb2.TaskAccepted] = []
        prefix = str(task_id_prefix or f"{self.job_id}-task").strip()
        with self._future_lock:
            for payload in payloads:
                task_id = self._next_task_id()
                if prefix:
                    task_id = f"{prefix}-{task_id.rsplit('-', 1)[-1]}"
                future = self._executor.submit(
                    self._submit_one,
                    task_id=task_id,
                    payload=dict(payload or {}),
                    method_name=method_name,
                    timeout_sec=timeout_sec,
                )
                self._futures[task_id] = future
                accepted.append(pb2.TaskAccepted(task_id=task_id, status=pb2.TASK_STATUS_QUEUED))
        return pb2.SubmitTasksResponse(ok=True, accepted=accepted, rejected=[], node_credit=0)

    def wait_for_results(
        self,
        *,
        expected_count: int = 0,
        timeout_sec: float = 30.0,
        wait_ms: int = 500,
        limit: int = 100,
        job_id: str = "",
    ) -> Sequence[pb2.TaskResult]:
        del wait_ms, limit, job_id
        deadline = time.time() + max(0.1, float(timeout_sec))
        results: List[pb2.TaskResult] = []
        target = max(0, int(expected_count or 0))
        while time.time() < deadline and (target <= 0 or len(results) < target):
            remaining = max(0.01, deadline - time.time())
            try:
                item = self._results.get(timeout=min(0.1, remaining))
            except queue.Empty:
                if target <= 0:
                    break
                continue
            results.append(item)
        return results

    def _iter_buffered_results(
        self,
        *,
        task_ids: Optional[Set[str]] = None,
        max_count: int = 0,
    ) -> List[pb2.TaskResult]:
        with self._buffer_lock:
            matched: List[pb2.TaskResult] = []
            kept: "deque[pb2.TaskResult]" = deque()
            while self._buffered_results:
                item = self._buffered_results.popleft()
                normalized = str(item.task_id or "").strip()
                if task_ids is not None and normalized not in task_ids:
                    kept.append(item)
                    continue
                if max_count > 0 and len(matched) >= max_count:
                    kept.append(item)
                    continue
                matched.append(item)
            self._buffered_results = kept
            return matched

    def _iter_result_items(
        self,
        *,
        max_count: int = 0,
        timeout_sec: float = 30.0,
        task_ids: Optional[Set[str]] = None,
    ) -> Iterator[pb2.TaskResult]:
        deadline = time.time() + max(0.1, float(timeout_sec))
        yielded = 0
        while True:
            buffered = self._iter_buffered_results(task_ids=task_ids, max_count=(max_count - yielded if max_count > 0 else 0))
            for item in buffered:
                yielded += 1
                yield item
                if max_count > 0 and yielded >= max_count:
                    return

            remaining = max(0.01, deadline - time.time())
            if remaining <= 0:
                return
            try:
                item = self._results.get(timeout=min(0.1, remaining))
            except queue.Empty:
                return
            normalized = str(item.task_id or "").strip()
            if task_ids is not None and normalized not in task_ids:
                with self._buffer_lock:
                    self._buffered_results.append(item)
                continue
            yielded += 1
            yield item
            if max_count > 0 and yielded >= max_count:
                return

    def _collect_data_for_task_ids(self, task_ids: Set[str], *, timeout_sec: float = 30.0) -> List[Tuple[str, Any]]:
        adapter = _PoolResultAdapter(self)
        out: List[Tuple[str, Any]] = []
        for item in self._iter_result_items(max_count=len(task_ids), timeout_sec=timeout_sec, task_ids=set(task_ids)):
            out.append((str(item.task_id), adapter.fetch_result_data(item)))
        return out

    def wait_for_data(
        self,
        *,
        expected_count: int = 0,
        timeout_sec: float = 30.0,
    ) -> Sequence[Any]:
        results = self.wait_for_results(expected_count=expected_count, timeout_sec=timeout_sec)
        return _resolve_task_results_data(_PoolResultAdapter(self), results)

    def submit_values(
        self,
        values: Sequence[Any],
        *,
        arg_name: str = "value",
        task_method: str = "",
        **shared_kwargs,
    ) -> pb2.SubmitTasksResponse:
        normalized_arg = str(arg_name or "value").strip() or "value"
        payloads = [{normalized_arg: value, **dict(shared_kwargs)} for value in values]
        return self.submit_payloads(payloads, task_method=task_method)

    def imap_unordered(
        self,
        payloads: Iterable[Dict[str, object]],
        *,
        task_method: str = "",
        max_in_flight: int = 32,
        receive_batch: int = 1,
        submit_timeout_sec: float = 60.0,
        result_timeout_sec: float = 30.0,
        wait_ms: int = 500,
    ) -> Iterator[Tuple[str, Any]]:
        if self._closed:
            raise RuntimeError("task pool session is closed")
        method_name = str(task_method or self._task_method).strip() or self._task_method
        max_pending = max(1, int(max_in_flight or 1))
        max_receive = max(1, int(receive_batch or 1))
        payload_iter = iter(payloads)
        stream_task_ids: Set[str] = set()
        input_exhausted = False
        adapter = _PoolResultAdapter(self)

        while True:
            while not input_exhausted and len(stream_task_ids) < max_pending:
                try:
                    payload = next(payload_iter)
                except StopIteration:
                    input_exhausted = True
                    break
                resp = self.submit_payloads([dict(payload or {})], task_method=method_name, timeout_sec=submit_timeout_sec)
                if len(resp.accepted) != 1:
                    raise RuntimeError(
                        f"imap_unordered expected exactly one accepted task per payload, "
                        f"got accepted={len(resp.accepted)} rejected={len(resp.rejected)}"
                    )
                stream_task_ids.add(str(resp.accepted[0].task_id))

            if not stream_task_ids:
                return

            received_any = False
            for item in self._iter_result_items(
                max_count=max_receive,
                timeout_sec=result_timeout_sec,
                task_ids=set(stream_task_ids),
            ):
                received_any = True
                stream_task_ids.discard(str(item.task_id))
                yield str(item.task_id), adapter.fetch_result_data(item)

            if not received_any and stream_task_ids:
                raise TimeoutError(
                    f"imap_unordered did not receive results before timeout; pending_task_ids={sorted(stream_task_ids)}"
                )

    def map(
        self,
        values: Sequence[Any],
        *,
        arg_name: str = "value",
        task_method: str = "",
        timeout_sec: float = 30.0,
        **shared_kwargs,
    ) -> Sequence[Any]:
        resp = self.submit_values(values, arg_name=arg_name, task_method=task_method, **shared_kwargs)
        return self.wait_for_data(expected_count=len(resp.accepted), timeout_sec=timeout_sec)

    def cancel_job(
        self,
        *,
        reason: str = "",
        job_id: str = "",
    ) -> pb2.CancelJobResponse:
        del reason, job_id
        cancelled = 0
        with self._future_lock:
            for task_id, future in list(self._futures.items()):
                if future.cancel():
                    cancelled += 1
                    self._results.put(
                        pb2.TaskResult(
                            task_id=task_id,
                            job_id=self.job_id,
                            status=pb2.TASK_STATUS_CANCELLED,
                            attempt=1,
                            started_at=_now_timestamp(),
                            finished_at=_now_timestamp(),
                            error=pb2.TaskError(type="Cancelled", message="cancelled before dispatch"),
                        )
                    )
        return pb2.CancelJobResponse(
            ok=True,
            queued_cancelled=cancelled,
            running_marked=0,
            already_done=0,
            not_found=0,
        )

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._executor.shutdown(wait=False, cancel_futures=True)
        self._group.close(end_services=True, reason="task pool session close")

    def __enter__(self) -> "DedicatedTaskServiceSession":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def __getattr__(self, name: str):
        if name.startswith("_"):
            raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")
        return _TaskPoolCallProxy(session=self, method_name=self._ensure_method(name))

    def call_sync(self, method: str, **kwargs) -> Any:
        normalized = self._ensure_method(method)
        return getattr(self, normalized).sync(**kwargs)

    async def call(self, method: str, **kwargs) -> Any:
        normalized = self._ensure_method(method)
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, lambda: getattr(self, normalized).sync(**kwargs))

    def __repr__(self) -> str:
        return f"<DedicatedTaskServiceSession methods={self.methods} nodes={len(self.node_ids)}>"

    @classmethod
    def from_infocenter(
        cls,
        *,
        infocenter_target: str,
        job_id: str = "",
        owner_client_id: Optional[str] = None,
        service_name: Optional[str] = None,
        func: Optional[Callable] = None,
        module: Optional[Any] = None,
        artifact_path: Union[str, os.PathLike[str], Sequence[Union[str, os.PathLike[str]]]] = "",
        blob: Optional[bytes] = None,
        runtime: str = "py3",
        entry_module: str = "",
        entry_callable: str = "run",
        package_format: str = "",
        dependency_allowlist: Optional[Sequence[str]] = None,
        managed_global_names: Optional[Sequence[str]] = None,
        worker_count: int = 10,
        heartbeat_timeout_sec: int = 30,
        idle_ttl_sec: int = 0,
        chunk_size: int = OBJECT_CHUNK_SIZE_BYTES,
        healthy_only: bool = True,
        tags: Optional[Sequence[str]] = None,
        node_ids: Optional[Sequence[str]] = None,
        node_instance_ids: Optional[Sequence[str]] = None,
        node_count: int = 0,
        node_limit: int = 100,
        allow_partial: bool = True,
        min_success_nodes: int = 1,
        timeout_sec: float = 10.0,
        session_cache_dir: str = "",
    ) -> "DedicatedTaskServiceSession":
        effective_service_name = str(service_name or "").strip() or f"task-pool-{uuid.uuid4().hex[:12]}"
        group = ServiceGroup.deploy_from_infocenter(
            infocenter_target=infocenter_target,
            owner_client_id=owner_client_id,
            service_name=effective_service_name,
            func=func,
            module=module,
            artifact_path=artifact_path,
            blob=blob,
            runtime=runtime,
            entry_module=entry_module,
            entry_callable=entry_callable,
            package_format=package_format,
            export_mode="single",
            export_methods=[entry_callable],
            dependency_allowlist=dependency_allowlist,
            managed_global_names=managed_global_names,
            worker_count=worker_count,
            heartbeat_timeout_sec=heartbeat_timeout_sec,
            idle_ttl_sec=idle_ttl_sec,
            expose_http=True,
            chunk_size=chunk_size,
            healthy_only=healthy_only,
            tags=tags,
            node_ids=node_ids,
            node_instance_ids=node_instance_ids,
            node_count=node_count,
            node_limit=node_limit,
            allow_partial=allow_partial,
            min_success_nodes=min_success_nodes,
            timeout_sec=timeout_sec,
            ensure_unique_service_name=True,
            reuse_existing_same_code=False,
            replace_existing_if_code_changed=True,
            session_cache_dir=session_cache_dir,
        )
        return cls(group=group, task_method=entry_callable, job_id=job_id)


class _PoolResultAdapter:
    def __init__(self, session: DedicatedTaskServiceSession) -> None:
        self._session = session

    def fetch_result_data(self, task_result: pb2.TaskResult, *, target_path: str = ""):
        del target_path
        if task_result.result:
            return struct_to_dict(task_result.result)
        raise RuntimeError(task_result.error.message or "task failed")


@dataclass(frozen=True)
class TaskPoolItem:
    task_id: str
    node_id: str
    ok: bool
    status: int
    node_instance_id: str = ""
    data: Any = None
    error_type: str = ""
    error_message: str = ""


class TaskPoolSession:
    """Native dedicated task pool session backed by NodeControl task pool RPCs."""

    def __init__(
        self,
        *,
        pools: Dict[str, NativeTaskPoolClient],
        nodes: Dict[str, InfoCenterNode],
        task_method: str,
        job_id: str = "",
    ) -> None:
        self._pools = pools
        self.nodes = nodes
        self._task_method = str(task_method or "run").strip() or "run"
        self._job_id = str(job_id or f"pool-{uuid.uuid4().hex[:12]}").strip()
        self._closed = False
        self._submit_seq = 0
        self._submit_lock = threading.Lock()
        self._pool_cycle = 0
        self._pool_lock = threading.Lock()
        self._hb_stop = threading.Event()
        self._hb_thread: Optional[threading.Thread] = None
        self._hb_lock = threading.Lock()
        self.failed = False
        self.failures: Dict[str, str] = {}
        self._active_nodes: set[str] = set(self._pools.keys())
        self._pending_task_ids: set[str] = set()
        self._result_state_lock = threading.Lock()
        self._buffered_result_items: "deque[Tuple[str, pb2.TaskResult]]" = deque()
        self._exclusive_lock = threading.Lock()
        self._exclusive_mode = ""
        self._exclusive_owner_thread_id = 0
        self._exclusive_depth = 0

    @property
    def client_id(self) -> str:
        first = next(iter(self._pools.values()))
        return first.owner_client_id

    @property
    def job_id(self) -> str:
        return self._job_id

    @property
    def code_version(self) -> str:
        first = next(iter(self._pools.values()))
        return first.code_version

    @property
    def node_ids(self) -> Sequence[str]:
        return [self.nodes[key].node_id if key in self.nodes else key for key in self._pools.keys()]

    @property
    def node_instance_ids(self) -> Sequence[str]:
        return list(self._pools.keys())

    @property
    def methods(self) -> List[str]:
        return [self._task_method]

    def _ensure_method(self, method_name: str) -> str:
        normalized = str(method_name or "").strip()
        if not normalized:
            raise ValueError("method is required")
        if normalized != self._task_method:
            raise AttributeError(
                f"'{type(self).__name__}' has no method '{normalized}'. Available methods: {self.methods}"
            )
        return normalized

    def _next_task_id(self) -> str:
        with self._submit_lock:
            self._submit_seq += 1
            return f"{self.job_id}-task-{self._submit_seq:04d}"

    def _select_pool_node(self) -> str:
        node_ids = list(self._pools.keys())
        if not node_ids:
            raise RuntimeError("task pool has no node pools")
        with self._pool_lock:
            idx = self._pool_cycle % len(node_ids)
            self._pool_cycle += 1
        return node_ids[idx]

    def submit_payloads(
        self,
        payloads: Sequence[Dict[str, object]],
        *,
        task_method: str = "",
        timeout_sec: float = 60.0,
        job_id: str = "",
        task_id_prefix: str = "",
        timeout_hint_sec: int = 0,
        priority: int = 1,
        runtime_key: str = "",
    ) -> pb2.SubmitTasksResponse:
        del task_method, timeout_sec, job_id, runtime_key
        self._assert_session_available("submit_payloads")
        if self._closed:
            raise RuntimeError("task pool session is closed")
        accepted: List[pb2.TaskAccepted] = []
        rejected: List[pb2.TaskRejected] = []
        prefix = str(task_id_prefix or f"{self.job_id}-task").strip()
        grouped: Dict[str, List[pb2.TaskSubmitItem]] = {}
        for payload in payloads:
            task_id = self._next_task_id()
            if prefix:
                task_id = f"{prefix}-{task_id.rsplit('-', 1)[-1]}"
            _, payload_struct, _ = serialize_inline_payload(payload or {}, context="task pool payload")
            target_node_id = self._select_pool_node()
            grouped.setdefault(target_node_id, []).append(
                pb2.TaskSubmitItem(
                    task_id=task_id,
                    payload=payload_struct,
                    timeout_hint_sec=max(0, int(timeout_hint_sec)),
                    priority=max(1, int(priority)),
                )
            )
        for node_id, items in grouped.items():
            resp = self._pools[node_id].submit_tasks(items, job_id=self.job_id)
            accepted.extend(resp.accepted)
            rejected.extend(resp.rejected)
        with self._result_state_lock:
            self._pending_task_ids.update(str(item.task_id) for item in accepted if str(item.task_id).strip())
        return pb2.SubmitTasksResponse(ok=True, accepted=accepted, rejected=rejected, node_credit=0)

    def _mark_result_consumed(self, task_id: str) -> None:
        normalized = str(task_id or "").strip()
        if not normalized:
            return
        with self._result_state_lock:
            self._pending_task_ids.discard(normalized)

    def _pending_result_count(self) -> int:
        with self._result_state_lock:
            return len(self._pending_task_ids)

    def _is_pending_task_id(self, task_id: str) -> bool:
        normalized = str(task_id or "").strip()
        if not normalized:
            return False
        with self._result_state_lock:
            return normalized in self._pending_task_ids

    def _clear_pending_for_current_job(self) -> None:
        with self._result_state_lock:
            self._pending_task_ids.clear()
            self._buffered_result_items.clear()

    def _buffered_result_count(self) -> int:
        with self._result_state_lock:
            return len(self._buffered_result_items)

    def _assert_session_available(self, action: str) -> None:
        current = threading.get_ident()
        with self._exclusive_lock:
            if self._exclusive_mode and self._exclusive_owner_thread_id != current:
                raise RuntimeError(
                    f"task pool session is exclusively used by {self._exclusive_mode}; "
                    f"cannot run {action} concurrently"
                )

    def _assert_clean_for_exclusive(self, action: str) -> None:
        pending = self._pending_result_count()
        buffered = self._buffered_result_count()
        if pending > 0 or buffered > 0:
            raise RuntimeError(
                f"{action} requires a clean task pool session; "
                f"there are unfinished async tasks or unread results "
                f"(pending_task_ids={pending}, buffered_results={buffered}). "
                "Please receive outstanding async results first."
            )

    def _enter_exclusive_mode(self, mode: str, *, require_clean: bool = False) -> None:
        current = threading.get_ident()
        with self._exclusive_lock:
            if self._exclusive_mode:
                if self._exclusive_owner_thread_id == current and self._exclusive_mode == mode:
                    self._exclusive_depth += 1
                    return
                raise RuntimeError(
                    f"task pool session is exclusively used by {self._exclusive_mode}; cannot enter {mode}"
                )
            if require_clean:
                self._assert_clean_for_exclusive(mode)
            self._exclusive_mode = mode
            self._exclusive_owner_thread_id = current
            self._exclusive_depth = 1

    def _exit_exclusive_mode(self, mode: str) -> None:
        current = threading.get_ident()
        with self._exclusive_lock:
            if self._exclusive_mode != mode or self._exclusive_owner_thread_id != current:
                return
            self._exclusive_depth -= 1
            if self._exclusive_depth <= 0:
                self._exclusive_mode = ""
                self._exclusive_owner_thread_id = 0
                self._exclusive_depth = 0

    def _iter_buffered_result_items(
        self,
        *,
        task_ids: Optional[Set[str]] = None,
        max_count: int = 0,
    ) -> List[Tuple[str, pb2.TaskResult]]:
        with self._result_state_lock:
            matched: List[Tuple[str, pb2.TaskResult]] = []
            kept: "deque[Tuple[str, pb2.TaskResult]]" = deque()
            while self._buffered_result_items:
                node_id, item = self._buffered_result_items.popleft()
                normalized = str(item.task_id or "").strip()
                if not self._is_pending_task_id(normalized):
                    continue
                if task_ids is not None and normalized not in task_ids:
                    kept.append((node_id, item))
                    continue
                if max_count > 0 and len(matched) >= max_count:
                    kept.append((node_id, item))
                    continue
                matched.append((node_id, item))
            self._buffered_result_items = kept
            return matched

    def _iter_result_items(
        self,
        *,
        max_count: Optional[int] = None,
        timeout_sec: float = 30.0,
        wait_ms: int = 500,
        limit: int = 100,
        job_id: str = "",
        task_ids: Optional[Set[str]] = None,
    ) -> Iterator[Tuple[str, pb2.TaskResult]]:
        del job_id
        deadline = time.time() + max(0.1, float(timeout_sec))
        yielded = 0
        while time.time() < deadline:
            effective_target = self._pending_result_count() if max_count is None else max(0, int(max_count))
            if effective_target > 0 and yielded >= effective_target:
                return
            buffered = self._iter_buffered_result_items(
                task_ids=task_ids,
                max_count=(effective_target - yielded if effective_target > 0 else 0),
            )
            for node_id, item in buffered:
                self._mark_result_consumed(item.task_id)
                yielded += 1
                yield node_id, item
                if effective_target > 0 and yielded >= effective_target:
                    return
            any_result = False
            remaining_by_max = effective_target - yielded if effective_target > 0 else 0
            for node_id, pool in self._pools.items():
                per_pull_limit = max(1, int(limit or 100))
                if remaining_by_max > 0:
                    per_pull_limit = max(1, min(per_pull_limit, remaining_by_max))
                resp = pool.pull_results(limit=per_pull_limit, wait_ms=0, cursor="")
                if not resp.results:
                    continue
                any_result = True
                for item in resp.results:
                    normalized = str(item.task_id or "").strip()
                    if not self._is_pending_task_id(normalized):
                        continue
                    if task_ids is not None and normalized not in task_ids:
                        with self._result_state_lock:
                            self._buffered_result_items.append((node_id, item))
                        continue
                    self._mark_result_consumed(item.task_id)
                    yielded += 1
                    yield node_id, item
                    if effective_target > 0 and yielded >= effective_target:
                        return
                if effective_target > 0:
                    remaining_by_max = effective_target - yielded
                    if remaining_by_max <= 0:
                        return
            if self._pending_result_count() <= 0:
                return
            if not any_result:
                time.sleep(max(0.01, min(0.1, wait_ms / 1000.0 if wait_ms > 0 else 0.02)))

    def iter_results(
        self,
        *,
        max_count: Optional[int] = None,
        timeout_sec: float = 30.0,
        wait_ms: int = 500,
        limit: int = 100,
        job_id: str = "",
    ) -> Iterator[pb2.TaskResult]:
        self._assert_session_available("iter_results")
        for _node_id, item in self._iter_result_items(
            max_count=max_count,
            timeout_sec=timeout_sec,
            wait_ms=wait_ms,
            limit=limit,
            job_id=job_id,
        ):
            yield item

    def collect_results(
        self,
        *,
        max_count: Optional[int] = None,
        timeout_sec: float = 30.0,
        wait_ms: int = 500,
        limit: int = 100,
        job_id: str = "",
    ) -> List[pb2.TaskResult]:
        return list(
            self.iter_results(
                max_count=max_count,
                timeout_sec=timeout_sec,
                wait_ms=wait_ms,
                limit=limit,
                job_id=job_id,
            )
        )

    def iter_data(
        self,
        *,
        max_count: Optional[int] = None,
        timeout_sec: float = 30.0,
        wait_ms: int = 500,
        limit: int = 100,
        job_id: str = "",
        raise_on_error: bool = False,
        task_ids: Optional[Set[str]] = None,
    ) -> Iterator[Tuple[str, Any]]:
        self._assert_session_available("iter_data")
        for item in self.iter_items(
            max_count=max_count,
            timeout_sec=timeout_sec,
            wait_ms=wait_ms,
            limit=limit,
            job_id=job_id,
            task_ids=task_ids,
        ):
            if not item.ok:
                if raise_on_error:
                    raise RuntimeError(item.error_message or f"task failed: {item.task_id}")
                yield item.task_id, None
                continue
            yield item.task_id, item.data

    def collect_data(
        self,
        *,
        max_count: Optional[int] = None,
        timeout_sec: float = 30.0,
        wait_ms: int = 500,
        limit: int = 100,
        job_id: str = "",
        raise_on_error: bool = False,
        task_ids: Optional[Set[str]] = None,
    ) -> List[Tuple[str, Any]]:
        return list(
            self.iter_data(
                max_count=max_count,
                timeout_sec=timeout_sec,
                wait_ms=wait_ms,
                limit=limit,
                job_id=job_id,
                raise_on_error=raise_on_error,
                task_ids=task_ids,
            )
        )

    def iter_items(
        self,
        *,
        max_count: Optional[int] = None,
        timeout_sec: float = 30.0,
        wait_ms: int = 500,
        limit: int = 100,
        job_id: str = "",
        task_ids: Optional[Set[str]] = None,
    ) -> Iterator[TaskPoolItem]:
        self._assert_session_available("iter_items")
        for node_id, task_result in self._iter_result_items(
            max_count=max_count,
            timeout_sec=timeout_sec,
            wait_ms=wait_ms,
            limit=limit,
            job_id=job_id,
            task_ids=task_ids,
        ):
            if int(task_result.status) != int(pb2.TASK_STATUS_SUCCEEDED):
                error = task_result.error
                yield TaskPoolItem(
                    task_id=str(task_result.task_id or ""),
                    node_id=str(self.nodes.get(node_id).node_id if node_id in self.nodes else node_id),
                    node_instance_id=str(node_id),
                    ok=False,
                    status=int(task_result.status),
                    data=None,
                    error_type=str(error.type or ""),
                    error_message=str(error.message or f"task failed: {task_result.task_id}"),
                )
                continue
            try:
                data = self._pools[node_id]._client.fetch_result_data(task_result)  # noqa: SLF001
                yield TaskPoolItem(
                    task_id=str(task_result.task_id or ""),
                    node_id=str(self.nodes.get(node_id).node_id if node_id in self.nodes else node_id),
                    node_instance_id=str(node_id),
                    ok=True,
                    status=int(task_result.status),
                    data=data,
                )
            except Exception as exc:
                yield TaskPoolItem(
                    task_id=str(task_result.task_id or ""),
                    node_id=str(self.nodes.get(node_id).node_id if node_id in self.nodes else node_id),
                    node_instance_id=str(node_id),
                    ok=False,
                    status=int(task_result.status),
                    data=None,
                    error_type=exc.__class__.__name__,
                    error_message=str(exc),
                )

    def collect_items(
        self,
        *,
        max_count: Optional[int] = None,
        timeout_sec: float = 30.0,
        wait_ms: int = 500,
        limit: int = 100,
        job_id: str = "",
        task_ids: Optional[Set[str]] = None,
    ) -> List[TaskPoolItem]:
        return list(
            self.iter_items(
                max_count=max_count,
                timeout_sec=timeout_sec,
                wait_ms=wait_ms,
                limit=limit,
                job_id=job_id,
                task_ids=task_ids,
            )
        )

    def _collect_data_for_task_ids(self, task_ids: Set[str], *, timeout_sec: float = 30.0) -> List[Tuple[str, Any]]:
        out: List[Tuple[str, Any]] = []
        for node_id, item in self._iter_result_items(max_count=len(task_ids), timeout_sec=timeout_sec, task_ids=set(task_ids)):
            if int(item.status) != int(pb2.TASK_STATUS_SUCCEEDED):
                error = item.error
                raise RuntimeError(str(error.message or f"task failed: {item.task_id}"))
            out.append((str(item.task_id), self._pools[node_id]._client.fetch_result_data(item)))  # noqa: SLF001
        return out

    def wait_for_results(
        self,
        *,
        expected_count: int = 0,
        timeout_sec: float = 30.0,
        wait_ms: int = 500,
        limit: int = 100,
        job_id: str = "",
    ) -> Sequence[pb2.TaskResult]:
        self._assert_session_available("wait_for_results")
        del job_id
        deadline = time.time() + max(0.1, float(timeout_sec))
        results: List[pb2.TaskResult] = []
        seen: set[str] = set()
        while time.time() < deadline and (expected_count <= 0 or len(results) < expected_count):
            for pool in self._pools.values():
                resp = pool.pull_results(limit=limit, wait_ms=0, cursor="")
                for item in resp.results:
                    if item.task_id in seen:
                        continue
                    seen.add(item.task_id)
                    self._mark_result_consumed(item.task_id)
                    results.append(item)
            if expected_count > 0 and len(results) >= expected_count:
                break
            time.sleep(max(0.01, min(0.1, wait_ms / 1000.0 if wait_ms > 0 else 0.02)))
        return results

    def wait_for_data(
        self,
        *,
        expected_count: int = 0,
        timeout_sec: float = 30.0,
    ) -> Sequence[Any]:
        self._assert_session_available("wait_for_data")
        results = self.wait_for_results(expected_count=expected_count, timeout_sec=timeout_sec)
        return _resolve_task_results_data(_NativePoolResultAdapter(), results)

    def submit_values(
        self,
        values: Sequence[Any],
        *,
        arg_name: str = "value",
        task_method: str = "",
        **shared_kwargs,
    ) -> pb2.SubmitTasksResponse:
        normalized_arg = str(arg_name or "value").strip() or "value"
        payloads = [{normalized_arg: value, **dict(shared_kwargs)} for value in values]
        return self.submit_payloads(payloads, task_method=task_method)

    def imap_unordered(
        self,
        payloads: Iterable[Dict[str, object]],
        *,
        task_method: str = "",
        max_in_flight: int = 32,
        receive_batch: int = 1,
        submit_timeout_sec: float = 60.0,
        result_timeout_sec: float = 30.0,
        wait_ms: int = 500,
        raise_on_error: bool = True,
    ) -> Iterator[Tuple[str, Any]]:
        if self._closed:
            raise RuntimeError("task pool session is closed")
        self._enter_exclusive_mode("imap_unordered", require_clean=True)
        try:
            method_name = str(task_method or self._task_method).strip() or self._task_method
            max_pending = max(1, int(max_in_flight or 1))
            max_receive = max(1, int(receive_batch or 1))
            payload_iter = iter(payloads)
            input_exhausted = False

            while True:
                while not input_exhausted and self._pending_result_count() < max_pending:
                    try:
                        payload = next(payload_iter)
                    except StopIteration:
                        input_exhausted = True
                        break
                    resp = self.submit_payloads([dict(payload or {})], task_method=method_name, timeout_sec=submit_timeout_sec)
                    if len(resp.accepted) != 1:
                        raise RuntimeError(
                            f"imap_unordered expected exactly one accepted task per payload, "
                            f"got accepted={len(resp.accepted)} rejected={len(resp.rejected)}"
                        )

                if input_exhausted and self._pending_result_count() <= 0:
                    return

                received_any = False
                for task_id, data in self.iter_data(
                    max_count=max_receive if max_receive > 0 else None,
                    timeout_sec=result_timeout_sec,
                    wait_ms=wait_ms,
                    raise_on_error=raise_on_error,
                ):
                    received_any = True
                    yield str(task_id), data

                if not received_any and self._pending_result_count() > 0:
                    raise TimeoutError(
                        f"imap_unordered did not receive results before timeout; pending_task_ids={self._pending_result_count()}"
                    )
        finally:
            self._exit_exclusive_mode("imap_unordered")

    def map(
        self,
        values: Sequence[Any],
        *,
        arg_name: str = "value",
        task_method: str = "",
        timeout_sec: float = 30.0,
        **shared_kwargs,
    ) -> Sequence[Any]:
        resp = self.submit_values(values, arg_name=arg_name, task_method=task_method, **shared_kwargs)
        return self.wait_for_data(expected_count=len(resp.accepted), timeout_sec=timeout_sec)

    def cancel_job(
        self,
        *,
        reason: str = "",
        job_id: str = "",
    ) -> pb2.CancelJobResponse:
        self._assert_session_available("cancel_job")
        effective_job_id = str(job_id or self.job_id).strip()
        queued_cancelled = 0
        running_marked = 0
        already_done = 0
        not_found = 0
        for pool in self._pools.values():
            resp = pool.cancel_job(job_id=effective_job_id, reason=reason)
            queued_cancelled += int(resp.queued_cancelled or 0)
            running_marked += int(resp.running_marked or 0)
            already_done += int(resp.already_done or 0)
            not_found += int(resp.not_found or 0)
        if effective_job_id == self.job_id:
            self._clear_pending_for_current_job()
        return pb2.CancelJobResponse(
            ok=True,
            queued_cancelled=queued_cancelled,
            running_marked=running_marked,
            already_done=already_done,
            not_found=not_found,
        )

    def status_map(self) -> Dict[str, pb2.TaskPoolStatusInfo]:
        return {node_id: pool.get_status() for node_id, pool in self._pools.items()}

    def is_alive(self) -> bool:
        if self._closed:
            return False
        if self.failed:
            return False
        return any(node_id in self._active_nodes for node_id in self._pools)

    def _start_keepalive(self, interval_sec: Optional[float] = None) -> None:
        with self._hb_lock:
            if self._hb_thread is not None and self._hb_thread.is_alive():
                return
            self.failed = False
            self.failures = {}
            self._active_nodes = set(self._pools.keys())
            default_interval = min(
                max(1.0, float(min(pool.heartbeat_timeout_sec for pool in self._pools.values())) / 2.0),
                30.0,
            )
            wait_sec = max(0.5, float(interval_sec or default_interval))
            self._hb_stop.clear()
            self._hb_thread = threading.Thread(
                target=self._heartbeat_loop,
                args=(wait_sec,),
                name=f"task-pool-hb-{self.job_id}",
                daemon=True,
            )
            self._hb_thread.start()

    def _stop_keepalive(self) -> None:
        with self._hb_lock:
            self._hb_stop.set()
            thread = self._hb_thread
        if thread is not None:
            thread.join(timeout=1.0)
        with self._hb_lock:
            self._hb_thread = None

    def _heartbeat_loop(self, interval_sec: float) -> None:
        seq = 0
        next_tick = time.monotonic() + max(0.1, float(interval_sec))
        while not self._hb_stop.is_set():
            now = time.monotonic()
            wait_sec = max(0.0, next_tick - now)
            if self._hb_stop.wait(wait_sec):
                break
            next_tick += max(0.1, float(interval_sec))
            seq += 1
            for node_id, pool in self._pools.items():
                if node_id not in self._active_nodes:
                    continue
                try:
                    pool.heartbeat(seq=seq)
                    self.failures.pop(node_id, None)
                except Exception as exc:
                    self.failures[node_id] = repr(exc)
                    self._active_nodes.discard(node_id)
            if not self._active_nodes:
                self.failed = True
                self._hb_stop.set()
                break

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._stop_keepalive()
        for pool in self._pools.values():
            with contextlib.suppress(Exception):
                pool.close(reason="task pool session close")
            with contextlib.suppress(Exception):
                pool._client.close()  # noqa: SLF001

    def __enter__(self) -> "TaskPoolSession":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def __getattr__(self, name: str):
        if name.startswith("_"):
            raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")
        return _TaskPoolCallProxy(session=self, method_name=self._ensure_method(name))

    def call_sync(self, method: str, **kwargs) -> Any:
        normalized = self._ensure_method(method)
        return getattr(self, normalized).sync(**kwargs)

    async def call(self, method: str, **kwargs) -> Any:
        normalized = self._ensure_method(method)
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, lambda: getattr(self, normalized).sync(**kwargs))

    def __repr__(self) -> str:
        return f"<TaskPoolSession methods={self.methods} nodes={len(self.node_ids)}>"

    @classmethod
    def from_infocenter(
        cls,
        *,
        infocenter_target: str,
        job_id: str = "",
        owner_client_id: Optional[str] = None,
        pool_name: Optional[str] = None,
        func: Optional[Callable] = None,
        module: Optional[Any] = None,
        artifact_path: Union[str, os.PathLike[str], Sequence[Union[str, os.PathLike[str]]]] = "",
        blob: Optional[bytes] = None,
        runtime: str = "py3",
        entry_module: str = "",
        entry_callable: str = "run",
        package_format: str = "",
        dependency_allowlist: Optional[Sequence[str]] = None,
        managed_global_names: Optional[Sequence[str]] = None,
        worker_count: int = 1,
        heartbeat_timeout_sec: int = 30,
        idle_ttl_sec: int = 0,
        chunk_size: int = OBJECT_CHUNK_SIZE_BYTES,
        healthy_only: bool = True,
        tags: Optional[Sequence[str]] = None,
        node_ids: Optional[Sequence[str]] = None,
        node_instance_ids: Optional[Sequence[str]] = None,
        node_count: int = 0,
        node_limit: int = 100,
        timeout_sec: float = 10.0,
    ) -> "TaskPoolSession":
        entry_module = _normalize_entry_module_arg(entry_module)
        effective_blob, effective_filename = _prepare_code_blob(
            func=func,
            module=module,
            artifact_path=artifact_path,
            blob=blob,
        )
        if effective_blob is None:
            raise ValueError("blob, func, module or artifact_path is required")
        effective_package_format = _resolve_package_format(package_format, effective_filename, default="py")
        if not entry_module:
            if module is not None:
                entry_module = _default_entry_module_for_module(module)
            elif func is not None:
                entry_module = _default_entry_module_for_func(func)
            elif artifact_path:
                entry_module = _infer_entry_module_from_artifact_path(artifact_path)
        if not entry_module and effective_package_format == "py":
            entry_module = _default_entry_module_for_package(package_format=effective_package_format, entry_module=entry_module, fallback_stem="task_pool_artifact")

        effective_owner = str(owner_client_id or f"client-{_get_local_ip()}").strip()
        requested_count = max(0, int(node_count or 0))
        fetch_limit = requested_count if requested_count > 0 else node_limit
        with InfoCenterClient(infocenter_target, timeout_sec=timeout_sec) as infocenter:
            selected_nodes = list(
                infocenter.select_task_nodes(
                    healthy_only=healthy_only,
                    tags=tags,
                    node_ids=node_ids,
                    node_instance_ids=node_instance_ids,
                    node_count=fetch_limit,
                    limit=node_limit,
                    require_credit=False,
                    preferred_runtime_key="",
                    runtime=runtime,
                )
            )
        if not selected_nodes:
            raise RuntimeError("no task pool nodes selected from InfoCenter")
        desired_nodes = selected_nodes[:requested_count] if requested_count > 0 else selected_nodes

        pools: Dict[str, NativeTaskPoolClient] = {}
        nodes: Dict[str, InfoCenterNode] = {}
        for node in desired_nodes:
            client = NodeControlClient(node.control_addr, timeout_sec=timeout_sec)
            pool = client.create_task_pool_from_bytes(
                owner_client_id=effective_owner,
                pool_name=str(pool_name or f"task-pool-{uuid.uuid4().hex[:10]}"),
                blob=effective_blob,
                runtime=runtime,
                entry_module=entry_module,
                entry_callable=entry_callable,
                package_format=effective_package_format,
                dependency_allowlist=dependency_allowlist,
                managed_global_names=managed_global_names,
                worker_count=worker_count,
                heartbeat_timeout_sec=heartbeat_timeout_sec,
                idle_ttl_sec=idle_ttl_sec,
                chunk_size=chunk_size,
            )
            node_key = _node_instance_key_from_node(node)
            pools[node_key] = pool
            nodes[node_key] = node
        session = cls(pools=pools, nodes=nodes, task_method=entry_callable, job_id=job_id)
        session._start_keepalive()
        return session


class _NativePoolResultAdapter:
    def fetch_result_data(self, task_result: pb2.TaskResult, *, target_path: str = ""):
        del target_path
        if task_result.result:
            return struct_to_dict(task_result.result)
        raise RuntimeError(task_result.error.message or "task failed")


@dataclass
class DiscoveryCallError(Exception):
    status_code: int
    data: Dict[str, object]

    def __str__(self) -> str:
        return str(self.data.get("error", f"http {self.status_code}"))


class _DiscoveryRouteCache:
    def __init__(
        self,
        *,
        infocenter_target: str,
        timeout_sec: float = 10.0,
        refresh_interval_sec: float = 3.0,
        failure_threshold: int = 3,
        open_sec: float = 5.0,
        route_limit: int = 500,
    ) -> None:
        self.infocenter_target = str(infocenter_target or "").strip()
        self.timeout_sec = max(0.1, float(timeout_sec))
        self.refresh_interval_sec = max(0.2, float(refresh_interval_sec))
        self.failure_threshold = max(1, int(failure_threshold))
        self.open_sec = max(0.1, float(open_sec))
        self.route_limit = max(1, int(route_limit))

        self._lock = threading.Lock()
        self._snapshots: Dict[str, _ServiceRouteSnapshot] = {}
        self._local_state: Dict[Tuple[str, str], _RouteLocalState] = {}
        self._route_index: Dict[str, int] = {}
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._stop_event.clear()
            self._thread = threading.Thread(
                target=self._refresh_loop,
                name="discovery-route-cache",
                daemon=True,
            )
            self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        thread = self._thread
        if thread is not None:
            thread.join(timeout=1.0)
        with self._lock:
            self._thread = None

    def _refresh_loop(self) -> None:
        while not self._stop_event.wait(self.refresh_interval_sec):
            with self._lock:
                service_names = list(self._snapshots.keys())
            for service_name in service_names:
                try:
                    self.refresh(service_name, force=True)
                except Exception:
                    continue

    def refresh(self, service_name: str, *, force: bool = False) -> Sequence[InfoCenterServiceRoute]:
        name = str(service_name or "").strip()
        if not name:
            raise ValueError("service_name is required")
        with InfoCenterClient(self.infocenter_target, timeout_sec=self.timeout_sec) as client:
            rows = list(
                client.list_service_routes(
                    service_name=name,
                    healthy_only=True,
                    limit=self.route_limit,
                )
            )
        snapshot = _ServiceRouteSnapshot(
            service_name=name,
            routes=rows,
            refreshed_at=datetime.now(timezone.utc),
        )
        with self._lock:
            if force or name not in self._snapshots or rows:
                self._snapshots[name] = snapshot
        return rows

    def get_routes(self, service_name: str) -> Sequence[InfoCenterServiceRoute]:
        name = str(service_name or "").strip()
        if not name:
            raise ValueError("service_name is required")
        with self._lock:
            snapshot = self._snapshots.get(name)
        if snapshot is None:
            return list(self.refresh(name, force=True))
        return list(snapshot.routes)

    def snapshot_info(self, service_name: str) -> Dict[str, object]:
        routes = list(self.get_routes(service_name))
        with self._lock:
            snapshot = self._snapshots.get(str(service_name or "").strip())
        return {
            "service_name": str(service_name or "").strip(),
            "refreshed_at": snapshot.refreshed_at.isoformat() if snapshot is not None else "",
            "route_count": len(routes),
            "routes": routes,
        }

    def select_route(
        self,
        service_name: str,
        *,
        exclude_service_ids: Optional[Set[str]] = None,
        force_refresh: bool = False,
        strategy: str = "least_inflight",
    ) -> InfoCenterServiceRoute:
        name = str(service_name or "").strip()
        routes = list(self.refresh(name, force=True)) if force_refresh else list(self.get_routes(name))
        excluded = exclude_service_ids or set()
        candidates = [
            route
            for route in routes
            if route.node_healthy
            and route.status == pb2.SERVICE_STATUS_RUNNING
            and route.http_base_url
            and route.service_id not in excluded
            and self._route_available(name, route.service_id)
        ]
        if not candidates:
            raise RuntimeError(f"no available route for service_name={name}")
        if strategy == "round_robin":
            candidates.sort(key=lambda route: (_node_instance_key_from_route(route), route.service_id))
            with self._lock:
                idx = self._route_index.get(name, 0)
                self._route_index[name] = idx + 1
            return candidates[idx % len(candidates)]
        if strategy != "least_inflight":
            raise ValueError("strategy must be one of: least_inflight, round_robin")
        candidates.sort(
            key=lambda route: (int(route.in_flight), -int(route.alive_workers), _node_instance_key_from_route(route), route.service_id)
        )
        return candidates[0]

    def _route_available(self, service_name: str, service_id: str) -> bool:
        key = (service_name, service_id)
        now = time.monotonic()
        with self._lock:
            state = self._local_state.get(key)
            if state is None:
                return True
            return now >= state.open_until_monotonic

    def mark_success(self, route: InfoCenterServiceRoute) -> None:
        key = (route.service_name, route.service_id)
        with self._lock:
            state = self._local_state.get(key)
            if state is None:
                return
            state.consecutive_failures = 0
            state.open_until_monotonic = 0.0
            state.last_error = ""

    def mark_failure(self, route: InfoCenterServiceRoute, error: str) -> None:
        key = (route.service_name, route.service_id)
        with self._lock:
            state = self._local_state.get(key)
            if state is None:
                state = _RouteLocalState()
                self._local_state[key] = state
            state.consecutive_failures += 1
            state.last_error = str(error or "")
            if state.consecutive_failures >= self.failure_threshold:
                state.open_until_monotonic = time.monotonic() + self.open_sec


def _serialize_route(route: InfoCenterServiceRoute) -> Dict[str, object]:
    return {
        "service_name": route.service_name,
        "service_id": route.service_id,
        "node_instance_id": route.node_instance_id,
        "node_id": route.node_id,
        "control_addr": route.control_addr,
        "node_healthy": route.node_healthy,
        "worker_count": route.worker_count,
        "alive_workers": route.alive_workers,
        "in_flight": route.in_flight,
        "http_base_url": route.http_base_url,
        "status": int(route.status),
        "lease_expire_at": route.lease_expire_at.isoformat(),
    }


def _call_route_http(
    route: InfoCenterServiceRoute,
    *,
    method: str,
    payload: Dict[str, object],
    timeout_sec: float,
    service_token: str,
) -> Dict[str, object]:
    url = f"{route.http_base_url}/call/{quote(method, safe='')}?timeout_sec={max(0.1, timeout_sec):.3f}"
    headers = {"Content-Type": "application/json"}
    if service_token:
        headers["X-Service-Token"] = service_token
    serialized_payload = _serialize_http_call_payload(payload, context="service call payload")
    req = Request(
        url=url,
        method="POST",
        headers=headers,
        data=json.dumps(serialized_payload).encode("utf-8"),
    )
    try:
        with urlopen(req, timeout=max(2.0, timeout_sec + 1.0)) as resp:
            data = _normalize_http_response_body(
                json.loads(resp.read().decode("utf-8") or "{}"),
                control_addr=route.control_addr,
            )
    except HTTPError as exc:
        try:
            data = _normalize_http_response_body(json.loads((exc.read() or b"{}").decode("utf-8") or "{}"))
        except Exception:
            data = {"ok": False, "error": exc.reason}
        raise DiscoveryCallError(status_code=exc.code, data=data) from exc
    except Exception as exc:
        raise DiscoveryCallError(status_code=502, data={"ok": False, "error": repr(exc)}) from exc
    if not data.get("ok", False):
        raise DiscoveryCallError(status_code=502, data=data)
    return data


def _is_route_failure(exc: DiscoveryCallError) -> bool:
    if exc.status_code == 502:
        return True
    if exc.status_code not in (404, 409, 500):
        return False
    msg = str(exc.data.get("error", "") or "").lower()
    return any(text in msg for text in ("service not found", "service not running", "service executor stopped", "artifact missing"))


class DiscoveryServiceClient:
    """Client-side service discovery caller.

    通过 InfoCenter 查 route，再直接调用节点上的 service_id HTTP 数据面。
    带本地 route cache、后台刷新和失败切换，整体行为尽量对齐 Gateway。
    """

    def __init__(
        self,
        infocenter_target: str,
        *,
        timeout_sec: float = 10.0,
        service_token: str = "",
        refresh_interval_sec: float = 3.0,
        failure_threshold: int = 3,
        open_sec: float = 5.0,
        route_limit: int = 500,
    ) -> None:
        self.infocenter_target = str(infocenter_target or "").strip()
        self.timeout_sec = max(0.1, float(timeout_sec))
        self.service_token = str(service_token or "").strip()
        self._route_cache = _DiscoveryRouteCache(
            infocenter_target=self.infocenter_target,
            timeout_sec=self.timeout_sec,
            refresh_interval_sec=refresh_interval_sec,
            failure_threshold=failure_threshold,
            open_sec=open_sec,
            route_limit=route_limit,
        )
        self._route_cache.start()

    def close(self) -> None:
        self._route_cache.stop()

    def __enter__(self) -> "DiscoveryServiceClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def refresh_routes(self, *, service_name: str, force: bool = False) -> Sequence[InfoCenterServiceRoute]:
        return list(self._route_cache.refresh(service_name, force=force))

    def list_routes(self, *, service_name: str) -> Sequence[InfoCenterServiceRoute]:
        return list(self._route_cache.get_routes(service_name))

    def get_status(self, *, service_name: str) -> Dict[str, object]:
        info = self._route_cache.snapshot_info(service_name)
        routes = info["routes"]
        return {
            "ok": True,
            "service_name": str(info["service_name"]),
            "refreshed_at": info["refreshed_at"],
            "route_count": int(info["route_count"]),
            "routes": [_serialize_route(route) for route in routes],
        }

    def download_result_to_file(self, response_or_data: object, *, target_path: str) -> Path:
        ref = _extract_result_ref(response_or_data)
        if ref is None:
            raise ValueError("service result is inline data; no download needed")
        if not ref.control_addr:
            raise RuntimeError("service result is missing control_addr for download")
        with NodeControlClient(ref.control_addr, timeout_sec=self.timeout_sec) as client:
            return client.download_result_to_file(ref, target_path=target_path)

    def fetch_result_data(self, response_or_data: object, *, target_path: str = ""):
        ref = _extract_result_ref(response_or_data)
        if ref is None:
            if isinstance(response_or_data, dict) and "data" in response_or_data:
                return response_or_data["data"]
            return response_or_data
        if not ref.control_addr:
            raise RuntimeError("service result is missing control_addr for download")
        with NodeControlClient(ref.control_addr, timeout_sec=self.timeout_sec) as client:
            return client.fetch_result_ref_data(ref, target_path=target_path)

    def list_methods(
        self,
        *,
        service_name: str,
        include_docs: bool = False,
        strategy: str = "least_inflight",
    ) -> Sequence[Dict[str, object]]:
        tried: Set[str] = set()
        try:
            route = self._route_cache.select_route(service_name, strategy=strategy)
            tried.add(route.service_id)
            methods = self._list_methods_via_route(route, include_docs=include_docs)
            self._route_cache.mark_success(route)
            return methods
        except Exception as exc:
            if tried:
                self._route_cache.mark_failure(route, str(exc))
            self._route_cache.refresh(service_name, force=True)
            retry_route = self._route_cache.select_route(service_name, exclude_service_ids=tried, strategy=strategy)
            methods = self._list_methods_via_route(retry_route, include_docs=include_docs)
            self._route_cache.mark_success(retry_route)
            return methods

    def call(
        self,
        *,
        service_name: str,
        method: str,
        payload: Optional[Dict[str, object]] = None,
        timeout_sec: float = 60.0,
        service_token: Optional[str] = None,
        strategy: str = "least_inflight",
    ) -> Dict[str, object]:
        name = str(service_name or "").strip()
        method_name = str(method or "").strip()
        if not name:
            raise ValueError("service_name is required")
        if not method_name:
            raise ValueError("method is required")
        token = self.service_token if service_token is None else str(service_token or "").strip()
        tried: Set[str] = set()
        route = self._route_cache.select_route(name, strategy=strategy)
        tried.add(route.service_id)
        try:
            resp = _call_route_http(
                route,
                method=method_name,
                payload=payload or {},
                timeout_sec=max(0.1, float(timeout_sec)),
                service_token=token,
            )
            self._route_cache.mark_success(route)
            return resp
        except DiscoveryCallError as exc:
            if not _is_route_failure(exc):
                raise RuntimeError(str(exc)) from exc
            self._route_cache.mark_failure(route, str(exc))
            self._route_cache.refresh(name, force=True)
            retry_route = self._route_cache.select_route(name, exclude_service_ids=tried, strategy=strategy)
            try:
                resp = _call_route_http(
                    retry_route,
                    method=method_name,
                    payload=payload or {},
                    timeout_sec=max(0.1, float(timeout_sec)),
                    service_token=token,
                )
                self._route_cache.mark_success(retry_route)
                return resp
            except DiscoveryCallError as retry_exc:
                if _is_route_failure(retry_exc):
                    self._route_cache.mark_failure(retry_route, str(retry_exc))
                raise RuntimeError(str(retry_exc)) from retry_exc

    def _list_methods_via_route(self, route: InfoCenterServiceRoute, *, include_docs: bool) -> List[Dict[str, object]]:
        with NodeControlClient(route.control_addr, timeout_sec=self.timeout_sec) as client:
            methods = client.list_service_methods(service_id=route.service_id, include_docs=include_docs)
        return [
            {
                "method": item.method,
                "qualified_name": item.qualified_name,
                "doc": item.doc,
            }
            for item in methods
        ]


@dataclass
class ServiceSessionClient:
    """Low-level client-side service session handle."""

    _client: "NodeControlClient" = field(repr=False)
    owner_client_id: str
    service_id: str
    service_token: str
    http_base_url: str
    heartbeat_timeout_sec: int
    worker_count: int
    status: int
    failed: bool = False
    last_error: str = ""
    heartbeat_failure_threshold: int = 3
    _hb_stop: threading.Event = field(default_factory=threading.Event, repr=False)
    _hb_thread: Optional[threading.Thread] = field(default=None, repr=False)
    _hb_lock: threading.Lock = field(default_factory=threading.Lock, repr=False)
    _hb_seq_lock: threading.Lock = field(default_factory=threading.Lock, repr=False)
    _hb_seq: int = field(default=0, repr=False)
    _hb_interval_sec: float = field(default=0.0, repr=False)
    _hb_consecutive_failures: int = field(default=0, repr=False)

    def _start_keepalive(self, interval_sec: Optional[float] = None) -> None:
        with self._hb_lock:
            if self._hb_thread is not None and self._hb_thread.is_alive():
                return
            self._hb_stop.clear()
            self.failed = False
            self.last_error = ""
            self._hb_consecutive_failures = 0
            default_interval = max(1.0, float(self.heartbeat_timeout_sec) / 2.0)
            self._hb_interval_sec = max(0.5, float(interval_sec if interval_sec is not None else default_interval))
            self._hb_thread = threading.Thread(
                target=self._keepalive_loop,
                name=f"svc-hb-{self.service_id[:8]}",
                daemon=True,
            )
            self._hb_thread.start()

    def _stop_keepalive(self) -> None:
        with self._hb_lock:
            self._hb_stop.set()
            thread = self._hb_thread
        if thread is not None:
            thread.join(timeout=2.0)
        with self._hb_lock:
            self._hb_thread = None

    def heartbeat(self) -> pb2.HeartbeatServiceResponse:
        with self._hb_seq_lock:
            self._hb_seq += 1
            seq = self._hb_seq
        resp = self._client.heartbeat_service(
            owner_client_id=self.owner_client_id,
            service_id=self.service_id,
            service_token=self.service_token,
            seq=seq,
        )
        self.status = resp.status
        self.failed = False
        self.last_error = ""
        self._hb_consecutive_failures = 0
        if resp.next_heartbeat_in_sec > 0:
            self._hb_interval_sec = float(resp.next_heartbeat_in_sec)
        return resp

    def end(self, reason: str = "client requested end") -> pb2.EndServiceResponse:
        self._stop_keepalive()
        resp = self._client.end_service(
            owner_client_id=self.owner_client_id,
            service_id=self.service_id,
            service_token=self.service_token,
            reason=reason,
        )
        self.status = resp.status
        return resp

    def get_status(self) -> pb2.ServiceStatusInfo:
        info = self._client.get_service_status(service_id=self.service_id)
        self.status = info.status
        return info

    def list_methods(self, *, include_docs: bool = False) -> Sequence[pb2.ServiceMethodInfo]:
        return self._client.list_service_methods(service_id=self.service_id, include_docs=include_docs)

    def call(
        self,
        method: str,
        payload: Dict[str, object],
        *,
        timeout_sec: float = 60.0,
        token: Optional[str] = None,
    ) -> Dict[str, object]:
        if not self.http_base_url:
            raise RuntimeError("service has no http_base_url; expose_http may be false")
        if not method:
            raise ValueError("method is required")

        params = urlencode({"timeout_sec": f"{max(0.1, float(timeout_sec)):.3f}"})
        url = f"{self.http_base_url}/call/{quote(method, safe='')}?{params}"
        headers = {"Content-Type": "application/json"}
        auth_token = self.service_token if token is None else token
        if auth_token:
            headers["X-Service-Token"] = auth_token
        serialized_payload = _serialize_http_call_payload(payload, context="service call payload")
        req = Request(
            url=url,
            method="POST",
            headers=headers,
            data=json.dumps(serialized_payload).encode("utf-8"),
        )
        try:
            with urlopen(req, timeout=max(2.0, float(timeout_sec) + 1.0)) as resp:
                body = _normalize_http_response_body(
                    json.loads(resp.read().decode("utf-8") or "{}"),
                    control_addr=self._client.target,
                )
        except HTTPError as exc:
            raw = exc.read().decode("utf-8") if hasattr(exc, "read") else ""
            msg = raw or str(exc)
            raise RuntimeError(f"call failed: {msg}") from exc
        if not body.get("ok", False):
            raise RuntimeError(f"call failed: {body.get('error', 'unknown error')}")
        return body

    def download_result_to_file(self, response_or_data: object, *, target_path: str) -> Path:
        ref = _extract_result_ref(response_or_data)
        if ref is None:
            raise ValueError("service result is inline data; no download needed")
        return self._client.download_result_to_file(ref, target_path=target_path)

    def fetch_result_data(self, response_or_data: object, *, target_path: str = ""):
        ref = _extract_result_ref(response_or_data)
        if ref is None:
            if isinstance(response_or_data, dict) and "data" in response_or_data:
                return response_or_data["data"]
            return response_or_data
        return self._client.fetch_result_ref_data(ref, target_path=target_path)

    def update_globals(self, values: Dict[str, object]) -> pb2.UpdateServiceGlobalsResponse:
        return self.update_globals_prepared(values)

    def update_globals_prepared(self, prepared_values: Dict[str, object]) -> pb2.UpdateServiceGlobalsResponse:
        return self._client.update_service_globals(
            owner_client_id=self.owner_client_id,
            service_id=self.service_id,
            service_token=self.service_token,
            values=prepared_values,
        )

    def _keepalive_loop(self) -> None:
        next_tick = time.monotonic() + max(0.5, float(self._hb_interval_sec))
        while not self._hb_stop.is_set():
            now = time.monotonic()
            wait_sec = max(0.0, next_tick - now)
            if self._hb_stop.wait(wait_sec):
                break
            try:
                self.heartbeat()
            except Exception as exc:
                self._hb_consecutive_failures += 1
                self.last_error = repr(exc)
                if self._hb_consecutive_failures >= max(1, int(self.heartbeat_failure_threshold or 1)):
                    self.failed = True
                    self.status = pb2.SERVICE_STATUS_STOPPED
                    break
                # Keep trying for a short window before declaring the owner-side session failed.
                self._hb_interval_sec = min(1.0, max(0.1, self._hb_interval_sec))
            next_tick = time.monotonic() + max(0.5, float(self._hb_interval_sec))


class NodeControlClient:
    """Thin gRPC client wrapper for NodeControl service."""

    def __init__(self, target: str, *, timeout_sec: float = 10.0) -> None:
        self.target = target
        self.timeout_sec = max(0.1, float(timeout_sec))
        self.channel = grpc.insecure_channel(target, options=grpc_channel_options())
        self.stub = pb2_grpc.NodeControlServiceStub(self.channel)

    def close(self) -> None:
        self.channel.close()

    def __enter__(self) -> "NodeControlClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def upload_code_from_file(
        self,
        *,
        client_id: str,
        artifact_path: str,
        runtime: str = "py3",
        entry_module: str = "",
        entry_callable: str = "run",
        package_format: str = "",
        export_mode: str = "single",
        export_methods: Optional[Sequence[str]] = None,
        dependency_allowlist: Optional[Sequence[str]] = None,
        managed_global_names: Optional[Sequence[str]] = None,
        code_token: str = "",
        chunk_size: int = OBJECT_CHUNK_SIZE_BYTES,
    ) -> pb2.UploadCodeResponse:
        path = Path(artifact_path)
        if not path.exists():
            raise FileNotFoundError(f"artifact_path not found: {artifact_path}")

        tmp_pkg: Optional[Path] = None
        upload_file = path
        inferred_format = _resolve_package_format(package_format, path.name)
        if path.is_dir():
            tmp_pkg = _package_directory_to_targz(path)
            upload_file = tmp_pkg
            inferred_format = package_format or "tar.gz"

        try:
            return self._upload_code_from_local_file(
                client_id=client_id,
                file_path=upload_file,
                runtime=runtime,
                entry_module=entry_module,
                entry_callable=entry_callable,
                package_format=inferred_format,
                export_mode=export_mode,
                export_methods=export_methods,
                dependency_allowlist=dependency_allowlist,
                managed_global_names=managed_global_names,
                code_token=code_token,
                chunk_size=chunk_size,
            )
        finally:
            if tmp_pkg is not None:
                tmp_pkg.unlink(missing_ok=True)

    def upload_code_from_bytes(
        self,
        *,
        client_id: str,
        blob: bytes,
        runtime: str = "py3",
        entry_module: str = "",
        entry_callable: str = "run",
        package_format: str = "",
        export_mode: str = "single",
        export_methods: Optional[Sequence[str]] = None,
        dependency_allowlist: Optional[Sequence[str]] = None,
        managed_global_names: Optional[Sequence[str]] = None,
        code_token: str = "",
        chunk_size: int = OBJECT_CHUNK_SIZE_BYTES,
    ) -> pb2.UploadCodeResponse:
        if not client_id:
            raise ValueError("client_id is required")
        effective_format = _resolve_package_format(package_format, default="py")
        effective_module = _default_entry_module_for_package(
            package_format=effective_format,
            entry_module=entry_module,
            fallback_stem="artifact",
        )
        export_spec = _build_export_spec(
            export_mode=export_mode,
            export_methods=export_methods,
        )
        digest = hashlib.sha256(blob).hexdigest()

        def _iter() -> Iterator[pb2.UploadCodeRequest]:
            yield pb2.UploadCodeRequest(
                meta=pb2.UploadCodeMeta(
                    client_id=client_id,
                    sha256=f"sha256:{digest}",
                    runtime=runtime,
                    entry_module=effective_module,
                    entry_callable=entry_callable or "run",
                    package_format=effective_format,
                    export_spec=export_spec,
                    dependency_allowlist=list(dependency_allowlist or ()),
                    managed_global_names=[str(name) for name in (managed_global_names or ()) if str(name).strip()],
                    code_token=str(code_token or "").strip(),
                )
            )
            for i in range(0, len(blob), max(1, int(chunk_size))):
                yield pb2.UploadCodeRequest(chunk=blob[i : i + chunk_size])

        resp = self.stub.UploadCode(_iter(), timeout=self.timeout_sec)
        if not resp.ok:
            raise RuntimeError(_err_msg(resp.error, "upload code failed"))
        return resp

    def upload_object_from_file(
        self,
        *,
        file_path: str,
        format: str = "",
        chunk_size: int = OBJECT_CHUNK_SIZE_BYTES,
    ) -> ObjectRef:
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"file_path not found: {file_path}")
        if not path.is_file():
            raise ValueError(f"file_path must be a file: {file_path}")
        return self._upload_object_from_local_file(
            file_path=path,
            format=normalize_object_format(format, source_name=path.name),
            chunk_size=chunk_size,
        )

    def upload_object_from_bytes(
        self,
        *,
        blob: bytes,
        format: str = "",
        chunk_size: int = OBJECT_CHUNK_SIZE_BYTES,
    ) -> ObjectRef:
        digest = hashlib.sha256(blob).hexdigest()
        object_id = object_id_from_sha256_hex(digest)
        effective_format = normalize_object_format(format, default="bin")

        def _iter() -> Iterator[pb2.UploadObjectRequest]:
            yield pb2.UploadObjectRequest(
                meta=pb2.UploadObjectMeta(
                    object_id=object_id,
                    format=effective_format,
                )
            )
            for i in range(0, len(blob), max(1, int(chunk_size))):
                yield pb2.UploadObjectRequest(chunk=blob[i : i + chunk_size])

        resp = self.stub.UploadObject(_iter(), timeout=self.timeout_sec)
        if not resp.ok:
            raise RuntimeError(_err_msg(resp.error, "upload object failed"))
        return ObjectRef(
            object_id=resp.object_id or object_id,
            format=resp.format or effective_format,
            size_bytes=int(resp.size_bytes or len(blob)),
        )

    def _upload_object_from_local_file(
        self,
        *,
        file_path: Path,
        format: str,
        chunk_size: int,
    ) -> ObjectRef:
        effective_format = normalize_object_format(format, source_name=file_path.name)
        digest = _sha256_file(file_path)
        object_id = object_id_from_sha256_hex(digest)

        def _iter() -> Iterator[pb2.UploadObjectRequest]:
            yield pb2.UploadObjectRequest(
                meta=pb2.UploadObjectMeta(
                    object_id=object_id,
                    format=effective_format,
                )
            )
            yield from (pb2.UploadObjectRequest(chunk=chunk) for chunk in _iter_file_chunks(file_path, chunk_size=chunk_size))

        resp = self.stub.UploadObject(_iter(), timeout=self.timeout_sec)
        if not resp.ok:
            raise RuntimeError(_err_msg(resp.error, "upload object failed"))
        return ObjectRef(
            object_id=resp.object_id or object_id,
            format=resp.format or effective_format,
            size_bytes=int(resp.size_bytes or file_path.stat().st_size),
        )

    def _upload_code_from_local_file(
        self,
        *,
        client_id: str,
        file_path: Path,
        runtime: str,
        entry_module: str,
        entry_callable: str,
        package_format: str,
        export_mode: str,
        export_methods: Optional[Sequence[str]],
        dependency_allowlist: Optional[Sequence[str]],
        managed_global_names: Optional[Sequence[str]],
        code_token: str,
        chunk_size: int,
    ) -> pb2.UploadCodeResponse:
        effective_format = _resolve_package_format(package_format, file_path.name)
        effective_module = _normalize_entry_module_arg(entry_module) or (
            file_path.stem if effective_format == "py" else ""
        )
        export_spec = _build_export_spec(
            export_mode=export_mode,
            export_methods=export_methods,
        )
        digest = _sha256_file(file_path)

        def _iter() -> Iterator[pb2.UploadCodeRequest]:
            yield pb2.UploadCodeRequest(
                meta=pb2.UploadCodeMeta(
                    client_id=client_id,
                    sha256=f"sha256:{digest}",
                    runtime=runtime,
                    entry_module=effective_module,
                    entry_callable=entry_callable or "run",
                    package_format=effective_format,
                    export_spec=export_spec,
                    dependency_allowlist=list(dependency_allowlist or ()),
                    managed_global_names=[str(name) for name in (managed_global_names or ()) if str(name).strip()],
                    code_token=str(code_token or "").strip(),
                )
            )
            yield from (pb2.UploadCodeRequest(chunk=chunk) for chunk in _iter_file_chunks(file_path, chunk_size=chunk_size))

        resp = self.stub.UploadCode(_iter(), timeout=self.timeout_sec)
        if not resp.ok:
            raise RuntimeError(_err_msg(resp.error, "upload code failed"))
        return resp

    def download_object_to_file(
        self,
        *,
        object_id: str,
        target_path: str,
    ) -> Path:
        path = Path(target_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("wb") as fh:
            stream = self.stub.DownloadObject(
                pb2.DownloadObjectRequest(object_id=str(object_id or "").strip()),
                timeout=self.timeout_sec,
            )
            try:
                for chunk in stream:
                    if chunk.chunk:
                        fh.write(chunk.chunk)
            finally:
                with contextlib.suppress(Exception):
                    stream.cancel()
        return path

    def download_object_bytes(self, *, object_id: str) -> bytes:
        out = bytearray()
        stream = self.stub.DownloadObject(
            pb2.DownloadObjectRequest(object_id=str(object_id or "").strip()),
            timeout=self.timeout_sec,
        )
        try:
            for chunk in stream:
                if chunk.chunk:
                    out.extend(chunk.chunk)
        finally:
            with contextlib.suppress(Exception):
                stream.cancel()
        return bytes(out)

    def download_result_to_file(self, result_ref: ResultRef, *, target_path: str) -> Path:
        return self.download_object_to_file(object_id=result_ref.object_id, target_path=target_path)

    def fetch_result_ref_data(self, result_ref: ResultRef, *, target_path: str = ""):
        log_payload_flow(
            "result_ref_fetch",
            format=result_ref.format,
            materialize_as=result_ref.materialize_as,
            target_path=(target_path or "<temp>"),
            summary=summarize_payload_flow_value(result_ref),
        )
        if target_path:
            return self.download_result_to_file(result_ref, target_path=target_path)
        suffix = Path(f"result{('.' + result_ref.format) if result_ref.format else ''}")
        tmp = tempfile.NamedTemporaryFile(prefix="pycloud-result-", suffix=suffix.suffix, delete=False)
        tmp_path = Path(tmp.name)
        tmp.close()
        try:
            self.download_result_to_file(result_ref, target_path=str(tmp_path))
            return _materialize_downloaded_result(tmp_path, result_ref=result_ref)
        except Exception:
            tmp_path.unlink(missing_ok=True)
            raise

    def fetch_result_data(self, task_result: pb2.TaskResult, *, target_path: str = ""):
        data = struct_to_dict(task_result.result)
        if not isinstance(data, ResultRef):
            return data
        return self.fetch_result_ref_data(data, target_path=target_path)

    def fetch_service_result_data(self, call_response: pb2.CallServiceResponse, *, target_path: str = ""):
        data = struct_to_dict(call_response.data)
        if not isinstance(data, ResultRef):
            return data
        return self.fetch_result_ref_data(data, target_path=target_path)

    def get_metrics(self) -> pb2.GetMetricsResponse:
        resp = self.stub.GetMetrics(
            pb2.GetMetricsRequest(),
            timeout=self.timeout_sec,
        )
        if not resp.ok:
            raise RuntimeError(_err_msg(resp.error, "get metrics failed"))
        return resp

    def update_runtime_globals(
        self,
        *,
        client_id: str,
        code_version: str,
        runtime_key: str,
        code_token: str,
        values: Dict[str, object],
    ) -> pb2.UpdateRuntimeGlobalsResponse:
        prepared_values = _prepare_managed_globals_values_for_upload([self], values)
        resp = self.stub.UpdateRuntimeGlobals(
            pb2.UpdateRuntimeGlobalsRequest(
                client_id=str(client_id or "").strip(),
                code_version=str(code_version or "").strip(),
                runtime_key=str(runtime_key or "").strip(),
                code_token=str(code_token or "").strip(),
                values=dict_to_struct(prepared_values),
            ),
            timeout=self.timeout_sec,
        )
        if not resp.ok:
            raise RuntimeError(_err_msg(resp.error, "update runtime globals failed"))
        return resp

    def create_service_from_file(
        self,
        *,
        owner_client_id: str,
        artifact_path: str,
        service_name: str = "",
        runtime: str = "py3",
        entry_module: str = "",
        entry_callable: str = "run",
        package_format: str = "",
        export_mode: str = "decorator",
        export_methods: Optional[Sequence[str]] = None,
        dependency_allowlist: Optional[Sequence[str]] = None,
        managed_global_names: Optional[Sequence[str]] = None,
        worker_count: int = 10,
        heartbeat_timeout_sec: int = 30,
        idle_ttl_sec: int = 0,
        expose_http: bool = True,
        chunk_size: int = OBJECT_CHUNK_SIZE_BYTES,
    ) -> ServiceSessionClient:
        path = Path(artifact_path)
        if not path.exists():
            raise FileNotFoundError(f"artifact_path not found: {artifact_path}")

        tmp_pkg: Optional[Path] = None
        upload_file = path
        inferred_format = _resolve_package_format(package_format, path.name)
        if path.is_dir():
            tmp_pkg = _package_directory_to_targz(path)
            upload_file = tmp_pkg
            inferred_format = package_format or "tar.gz"

        try:
            return self._create_service_from_local_file(
                owner_client_id=owner_client_id,
                service_name=service_name,
                file_path=upload_file,
                runtime=runtime,
                entry_module=entry_module,
                entry_callable=entry_callable,
                package_format=inferred_format,
                export_mode=export_mode,
                export_methods=export_methods,
                dependency_allowlist=dependency_allowlist,
                managed_global_names=managed_global_names,
                worker_count=worker_count,
                heartbeat_timeout_sec=heartbeat_timeout_sec,
                idle_ttl_sec=idle_ttl_sec,
                expose_http=expose_http,
                chunk_size=chunk_size,
            )
        finally:
            if tmp_pkg is not None:
                tmp_pkg.unlink(missing_ok=True)

    def create_service_from_paths(
        self,
        *,
        owner_client_id: str,
        root_dir: str,
        paths: Sequence[str],
        service_name: str = "",
        runtime: str = "py3",
        entry_module: str = "",
        entry_callable: str = "run",
        export_mode: str = "decorator",
        export_methods: Optional[Sequence[str]] = None,
        dependency_allowlist: Optional[Sequence[str]] = None,
        managed_global_names: Optional[Sequence[str]] = None,
        worker_count: int = 10,
        heartbeat_timeout_sec: int = 30,
        idle_ttl_sec: int = 0,
        expose_http: bool = True,
        chunk_size: int = OBJECT_CHUNK_SIZE_BYTES,
    ) -> ServiceSessionClient:
        tar_path = _package_paths_to_targz(root_dir=Path(root_dir), paths=paths)
        try:
            return self._create_service_from_local_file(
                owner_client_id=owner_client_id,
                service_name=service_name,
                file_path=tar_path,
                runtime=runtime,
                entry_module=entry_module,
                entry_callable=entry_callable,
                package_format="tar.gz",
                export_mode=export_mode,
                export_methods=export_methods,
                dependency_allowlist=dependency_allowlist,
                managed_global_names=managed_global_names,
                worker_count=worker_count,
                heartbeat_timeout_sec=heartbeat_timeout_sec,
                idle_ttl_sec=idle_ttl_sec,
                expose_http=expose_http,
                chunk_size=chunk_size,
            )
        finally:
            tar_path.unlink(missing_ok=True)

    def _create_service_from_local_file(
        self,
        *,
        owner_client_id: str,
        service_name: str,
        file_path: Path,
        runtime: str,
        entry_module: str,
        entry_callable: str,
        package_format: str,
        export_mode: str,
        export_methods: Optional[Sequence[str]],
        dependency_allowlist: Optional[Sequence[str]],
        managed_global_names: Optional[Sequence[str]],
        worker_count: int,
        heartbeat_timeout_sec: int,
        idle_ttl_sec: int,
        expose_http: bool,
        chunk_size: int,
    ) -> ServiceSessionClient:
        effective_module = _normalize_entry_module_arg(entry_module)
        effective_format = _resolve_package_format(package_format, file_path.name)
        if not effective_module and effective_format == "py":
            effective_module = file_path.stem
        digest = _sha256_file(file_path)
        export_spec = _build_export_spec(
            export_mode=export_mode,
            export_methods=export_methods,
        )

        def _iter() -> Iterator[pb2.CreateServiceRequest]:
            yield pb2.CreateServiceRequest(
                meta=pb2.CreateServiceMeta(
                    owner_client_id=owner_client_id,
                    service_name=service_name,
                    sha256=f"sha256:{digest}",
                    runtime=runtime,
                    entry_module=effective_module,
                    entry_callable=entry_callable or "run",
                    worker_count=max(1, int(worker_count)),
                    heartbeat_timeout_sec=max(1, int(heartbeat_timeout_sec)),
                    idle_ttl_sec=max(0, int(idle_ttl_sec)),
                    expose_http=bool(expose_http),
                    package_format=effective_format,
                    export_spec=export_spec,
                    dependency_allowlist=list(dependency_allowlist or ()),
                    managed_global_names=[str(name) for name in (managed_global_names or ()) if str(name).strip()],
                )
            )
            yield from (pb2.CreateServiceRequest(chunk=chunk) for chunk in _iter_file_chunks(file_path, chunk_size=chunk_size))

        resp = self.stub.CreateService(_iter(), timeout=self.timeout_sec)
        if not resp.ok:
            raise RuntimeError(_err_msg(resp.error, "create service failed"))
        return ServiceSessionClient(
            owner_client_id=owner_client_id,
            _client=self,
            service_id=resp.service_id,
            service_token=resp.service_token,
            http_base_url=resp.http_base_url,
            heartbeat_timeout_sec=resp.heartbeat_timeout_sec,
            worker_count=resp.worker_count,
            status=resp.status,
        )

    def create_service_from_bytes(
        self,
        *,
        owner_client_id: str,
        service_name: str,
        blob: bytes,
        runtime: str,
        entry_module: str,
        entry_callable: str,
        package_format: str = "",
        export_mode: str = "decorator",
        export_methods: Optional[Sequence[str]] = None,
        dependency_allowlist: Optional[Sequence[str]] = None,
        managed_global_names: Optional[Sequence[str]] = None,
        worker_count: int = 10,
        heartbeat_timeout_sec: int = 30,
        idle_ttl_sec: int = 0,
        expose_http: bool = True,
        chunk_size: int = OBJECT_CHUNK_SIZE_BYTES,
    ) -> ServiceSessionClient:
        if not owner_client_id:
            raise ValueError("owner_client_id is required")
        digest = hashlib.sha256(blob).hexdigest()
        effective_format = _resolve_package_format(package_format, default="py")
        effective_module = _default_entry_module_for_package(
            package_format=effective_format,
            entry_module=entry_module,
            fallback_stem="service_artifact",
        )
        export_spec = _build_export_spec(
            export_mode=export_mode,
            export_methods=export_methods,
        )

        def _iter() -> Iterator[pb2.CreateServiceRequest]:
            yield pb2.CreateServiceRequest(
                meta=pb2.CreateServiceMeta(
                    owner_client_id=owner_client_id,
                    service_name=service_name,
                    sha256=f"sha256:{digest}",
                    runtime=runtime,
                    entry_module=effective_module,
                    entry_callable=entry_callable or "run",
                    worker_count=max(1, int(worker_count)),
                    heartbeat_timeout_sec=max(1, int(heartbeat_timeout_sec)),
                    idle_ttl_sec=max(0, int(idle_ttl_sec)),
                    expose_http=bool(expose_http),
                    package_format=effective_format,
                    export_spec=export_spec,
                    dependency_allowlist=list(dependency_allowlist or ()),
                    managed_global_names=[str(name) for name in (managed_global_names or ()) if str(name).strip()],
                )
            )
            for i in range(0, len(blob), max(1, int(chunk_size))):
                yield pb2.CreateServiceRequest(chunk=blob[i : i + chunk_size])

        resp = self.stub.CreateService(_iter(), timeout=self.timeout_sec)
        if not resp.ok:
            raise RuntimeError(_err_msg(resp.error, "create service failed"))
        return ServiceSessionClient(
            _client=self,
            owner_client_id=owner_client_id,
            service_id=resp.service_id,
            service_token=resp.service_token,
            http_base_url=resp.http_base_url,
            heartbeat_timeout_sec=resp.heartbeat_timeout_sec,
            worker_count=resp.worker_count,
            status=resp.status,
        )

    def create_task_pool_from_bytes(
        self,
        *,
        owner_client_id: str,
        pool_name: str = "",
        blob: bytes,
        runtime: str,
        entry_module: str,
        entry_callable: str,
        package_format: str = "",
        dependency_allowlist: Optional[Sequence[str]] = None,
        managed_global_names: Optional[Sequence[str]] = None,
        worker_count: int = 1,
        heartbeat_timeout_sec: int = 30,
        idle_ttl_sec: int = 0,
        chunk_size: int = OBJECT_CHUNK_SIZE_BYTES,
    ) -> NativeTaskPoolClient:
        if not owner_client_id:
            raise ValueError("owner_client_id is required")
        digest = hashlib.sha256(blob).hexdigest()
        effective_format = _resolve_package_format(package_format, default="py")
        effective_module = _default_entry_module_for_package(
            package_format=effective_format,
            entry_module=entry_module,
            fallback_stem="task_pool_artifact",
        )
        log_payload_flow(
            "taskpool_create_grpc",
            owner_client_id=owner_client_id,
            pool_name=(pool_name or ""),
            runtime=runtime,
            entry_module=effective_module,
            entry_callable=(entry_callable or "run"),
            worker_count=max(1, int(worker_count)),
            blob_size=len(blob),
        )

        def _iter() -> Iterator[pb2.CreateTaskPoolRequest]:
            yield pb2.CreateTaskPoolRequest(
                meta=pb2.CreateTaskPoolMeta(
                    owner_client_id=owner_client_id,
                    pool_name=pool_name,
                    sha256=f"sha256:{digest}",
                    runtime=runtime,
                    entry_module=effective_module,
                    entry_callable=entry_callable or "run",
                    worker_count=max(1, int(worker_count)),
                    heartbeat_timeout_sec=max(1, int(heartbeat_timeout_sec)),
                    idle_ttl_sec=max(0, int(idle_ttl_sec)),
                    package_format=effective_format,
                    dependency_allowlist=list(dependency_allowlist or ()),
                    managed_global_names=[str(name) for name in (managed_global_names or ()) if str(name).strip()],
                )
            )
            for i in range(0, len(blob), max(1, int(chunk_size))):
                yield pb2.CreateTaskPoolRequest(chunk=blob[i : i + chunk_size])

        resp = self.stub.CreateTaskPool(_iter(), timeout=self.timeout_sec)
        if not resp.ok:
            raise RuntimeError(_err_msg(resp.error, "create task pool failed"))
        log_payload_flow(
            "taskpool_create_grpc_result",
            pool_id=resp.pool_id,
            code_version=resp.code_version,
            worker_count=resp.worker_count,
        )
        return NativeTaskPoolClient(
            _client=self,
            owner_client_id=owner_client_id,
            pool_id=resp.pool_id,
            pool_token=resp.pool_token,
            code_version=resp.code_version,
            worker_count=resp.worker_count,
            heartbeat_timeout_sec=resp.heartbeat_timeout_sec,
        )

    def submit_pool_tasks(
        self,
        *,
        pool_id: str,
        pool_token: str,
        tasks: Sequence[pb2.TaskSubmitItem],
        job_id: str = "",
    ) -> pb2.SubmitTasksResponse:
        log_payload_flow(
            "taskpool_submit_grpc",
            pool_id=str(pool_id or "").strip(),
            task_count=len(tasks),
            job_id=str(job_id or "").strip(),
        )
        resp = self.stub.SubmitPoolTasks(
            pb2.SubmitPoolTasksRequest(
                pool_id=str(pool_id or "").strip(),
                pool_token=str(pool_token or "").strip(),
                tasks=list(tasks),
                job_id=str(job_id or "").strip(),
            ),
            timeout=self.timeout_sec,
        )
        if not resp.ok:
            raise RuntimeError(_err_msg(resp.error, "submit pool tasks failed"))
        log_payload_flow(
            "taskpool_submit_grpc_result",
            pool_id=str(pool_id or "").strip(),
            accepted=len(resp.accepted),
            rejected=len(resp.rejected),
        )
        return resp

    def pull_pool_results(
        self,
        *,
        pool_id: str,
        pool_token: str,
        limit: int = 100,
        wait_ms: int = 0,
        cursor: str = "",
    ) -> pb2.PullResultsResponse:
        log_payload_flow(
            "taskpool_pull_results_grpc",
            pool_id=str(pool_id or "").strip(),
            limit=max(1, int(limit or 100)),
            wait_ms=max(0, int(wait_ms or 0)),
            cursor=str(cursor or "").strip(),
        )
        resp = self.stub.PullPoolResults(
            pb2.PullPoolResultsRequest(
                pool_id=str(pool_id or "").strip(),
                pool_token=str(pool_token or "").strip(),
                limit=max(1, int(limit or 100)),
                wait_ms=max(0, int(wait_ms or 0)),
                cursor=str(cursor or "").strip(),
            ),
            timeout=max(self.timeout_sec, max(0.1, float(wait_ms) / 1000.0) + 1.0),
        )
        if not resp.ok:
            raise RuntimeError(_err_msg(resp.error, "pull pool results failed"))
        log_payload_flow(
            "taskpool_pull_results_grpc_result",
            pool_id=str(pool_id or "").strip(),
            result_count=len(resp.results),
            next_cursor=resp.next_cursor,
        )
        return resp

    def close_task_pool(
        self,
        *,
        owner_client_id: str,
        pool_id: str,
        pool_token: str,
        reason: str = "",
    ) -> pb2.CloseTaskPoolResponse:
        resp = self.stub.CloseTaskPool(
            pb2.CloseTaskPoolRequest(
                owner_client_id=str(owner_client_id or "").strip(),
                pool_id=str(pool_id or "").strip(),
                pool_token=str(pool_token or "").strip(),
                reason=str(reason or ""),
            ),
            timeout=self.timeout_sec,
        )
        if not resp.ok or not resp.accepted:
            raise RuntimeError(_err_msg(resp.error, "close task pool failed"))
        return resp

    def heartbeat_task_pool(
        self,
        *,
        owner_client_id: str,
        pool_id: str,
        pool_token: str,
        seq: int = 0,
    ) -> pb2.HeartbeatTaskPoolResponse:
        resp = self.stub.HeartbeatTaskPool(
            pb2.HeartbeatTaskPoolRequest(
                owner_client_id=str(owner_client_id or "").strip(),
                pool_id=str(pool_id or "").strip(),
                seq=int(seq),
                timestamp=_now_timestamp(),
                pool_token=str(pool_token or "").strip(),
            ),
            timeout=self.timeout_sec,
        )
        if not resp.ok or not resp.accepted:
            raise RuntimeError(_err_msg(resp.error, "heartbeat task pool failed"))
        return resp

    def cancel_pool_job(
        self,
        *,
        pool_id: str,
        pool_token: str,
        job_id: str,
        reason: str = "",
    ) -> pb2.CancelJobResponse:
        resp = self.stub.CancelPoolJob(
            pb2.CancelPoolJobRequest(
                pool_id=str(pool_id or "").strip(),
                pool_token=str(pool_token or "").strip(),
                job_id=str(job_id or "").strip(),
                reason=str(reason or ""),
            ),
            timeout=self.timeout_sec,
        )
        if not resp.ok:
            raise RuntimeError(_err_msg(resp.error, "cancel pool job failed"))
        return resp

    def get_task_pool_status(
        self,
        *,
        pool_id: str,
        pool_token: str,
    ) -> pb2.TaskPoolStatusInfo:
        resp = self.stub.GetTaskPoolStatus(
            pb2.GetTaskPoolStatusRequest(
                pool_id=str(pool_id or "").strip(),
                pool_token=str(pool_token or "").strip(),
            ),
            timeout=self.timeout_sec,
        )
        if not resp.ok:
            raise RuntimeError(_err_msg(resp.error, "get task pool status failed"))
        return resp.pool

    def list_service_methods(self, *, service_id: str, include_docs: bool = False) -> Sequence[pb2.ServiceMethodInfo]:
        resp = self.stub.ListServiceMethods(
            pb2.ListServiceMethodsRequest(
                service_id=service_id,
                include_docs=bool(include_docs),
            ),
            timeout=self.timeout_sec,
        )
        if not resp.ok:
            raise RuntimeError(_err_msg(resp.error, "list service methods failed"))
        return list(resp.methods)

    def call_service(
        self,
        *,
        service_id: str,
        method: str,
        payload: Dict[str, object],
        timeout_sec: float = 60.0,
        service_token: str = "",
    ) -> pb2.CallServiceResponse:
        _, payload_struct, _ = serialize_inline_payload(payload or {}, context="service call payload")
        resp = self.stub.CallService(
            pb2.CallServiceRequest(
                service_id=service_id,
                method=method,
                payload=payload_struct,
                timeout_sec=max(0.1, float(timeout_sec)),
                service_token=service_token or "",
            ),
            timeout=max(self.timeout_sec, max(0.1, float(timeout_sec)) + 1.0),
        )
        if not resp.ok:
            reason = resp.task_error.message if resp.task_error and resp.task_error.message else _err_msg(resp.error, "call service failed")
            raise RuntimeError(reason)
        return resp

    def update_service_globals(
        self,
        *,
        owner_client_id: str,
        service_id: str,
        service_token: str,
        values: Dict[str, object],
    ) -> pb2.UpdateServiceGlobalsResponse:
        prepared_values = _prepare_managed_globals_values_for_upload([self], values)
        resp = self.stub.UpdateServiceGlobals(
            pb2.UpdateServiceGlobalsRequest(
                owner_client_id=str(owner_client_id or "").strip(),
                service_id=str(service_id or "").strip(),
                service_token=str(service_token or "").strip(),
                values=dict_to_struct(prepared_values),
            ),
            timeout=self.timeout_sec,
        )
        if not resp.ok:
            raise RuntimeError(_err_msg(resp.error, "update service globals failed"))
        return resp

    def heartbeat_service(
        self,
        *,
        owner_client_id: str,
        service_id: str,
        service_token: str,
        seq: int = 0,
    ) -> pb2.HeartbeatServiceResponse:
        resp = self.stub.HeartbeatService(
            pb2.HeartbeatServiceRequest(
                owner_client_id=owner_client_id,
                service_id=service_id,
                seq=seq,
                timestamp=_now_timestamp(),
                service_token=service_token,
            ),
            timeout=self.timeout_sec,
        )
        if not resp.ok or not resp.accepted:
            raise RuntimeError(_err_msg(resp.error, "heartbeat service failed"))
        return resp

    def end_service(
        self,
        *,
        owner_client_id: str,
        service_id: str,
        service_token: str,
        reason: str = "",
    ) -> pb2.EndServiceResponse:
        resp = self.stub.EndService(
            pb2.EndServiceRequest(
                owner_client_id=owner_client_id,
                service_id=service_id,
                reason=reason,
                service_token=service_token,
            ),
            timeout=self.timeout_sec,
        )
        if not resp.ok or not resp.accepted:
            raise RuntimeError(_err_msg(resp.error, "end service failed"))
        return resp

    def get_service_status(self, *, service_id: str) -> pb2.ServiceStatusInfo:
        resp = self.stub.GetServiceStatus(
            pb2.GetServiceStatusRequest(service_id=service_id),
            timeout=self.timeout_sec,
        )
        if not resp.ok:
            raise RuntimeError(_err_msg(resp.error, "get service status failed"))
        return resp.service

@dataclass
class ServiceGroup:
    """A deployed service group spread across multiple NodeControl nodes."""

    owner_client_id: str
    service_name: str
    sessions: Dict[str, ServiceSessionClient]
    nodes: Dict[str, InfoCenterNode]
    failures: Dict[str, str] = field(default_factory=dict)
    globals_digests: Dict[str, str] = field(default_factory=dict)
    breaker_enabled: bool = True
    breaker_failure_threshold: int = 3
    breaker_cooldown_sec: float = 15.0
    breaker_max_cooldown_sec: float = 120.0
    _clients: Dict[str, NodeControlClient] = field(default_factory=dict, repr=False)
    _session_cache_file: Optional[Path] = field(default=None, repr=False)
    _session_cache_lock: Optional[_ServiceSessionFileLock] = field(default=None, repr=False)
    _delete_session_cache_on_close: bool = field(default=False, repr=False)
    _artifact_code_version: str = field(default="", repr=False)
    _route_index: int = field(default=0, repr=False)
    _route_lock: threading.Lock = field(default_factory=threading.Lock, repr=False)
    _breaker_states: Dict[str, NodeCircuitState] = field(default_factory=dict, repr=False)

    @classmethod
    def deploy_from_infocenter(
        cls,
        *,
        infocenter_target: str,
        owner_client_id: Optional[str] = None,
        service_name: Optional[str] = None,
        func: Optional[Callable] = None,
        module: Optional[Any] = None,
        artifact_path: Union[str, os.PathLike[str], Sequence[Union[str, os.PathLike[str]]]] = "",
        blob: Optional[bytes] = None,
        runtime: str = "py3",
        entry_module: str = "",
        entry_callable: str = "run",
        package_format: str = "",
        export_mode: str = "decorator",
        export_methods: Optional[Sequence[str]] = None,
        dependency_allowlist: Optional[Sequence[str]] = None,
        managed_global_names: Optional[Sequence[str]] = None,
        worker_count: int = 10,
        heartbeat_timeout_sec: int = 30,
        idle_ttl_sec: int = 0,
        expose_http: bool = True,
        chunk_size: int = OBJECT_CHUNK_SIZE_BYTES,
        healthy_only: bool = True,
        tags: Optional[Sequence[str]] = None,
        node_ids: Optional[Sequence[str]] = None,
        node_instance_ids: Optional[Sequence[str]] = None,
        node_count: int = 0,
        node_limit: int = 100,
        allow_partial: bool = True,
        min_success_nodes: int = 1,
        timeout_sec: float = 10.0,
        ensure_unique_service_name: bool = True,
        reuse_existing_same_code: bool = True,
        replace_existing_if_code_changed: bool = True,
        session_cache_dir: str = "",
        breaker_enabled: bool = True,
        breaker_failure_threshold: int = 3,
        breaker_cooldown_sec: float = 15.0,
        breaker_max_cooldown_sec: float = 120.0,
    ) -> "ServiceGroup":
        """从 InfoCenter 发现节点并部署服务。

        Args:
            infocenter_target: InfoCenter 地址
            owner_client_id: 所有者客户端 ID
            service_name: 服务名称
            func: 函数对象（自动打包依赖，优先级最高）
            artifact_path: 单个文件、单个文件夹或文件/文件夹路径列表
            blob: 直接提供代码内容
            runtime: 运行时版本
            entry_module: 入口模块名
            entry_callable: 入口函数名
            package_format: 包格式 ("py", "zip", "tar.gz")
            export_mode: 导出模式 ("decorator", "explicit", "all", "single")
            export_methods: 显式导出的方法列表
            worker_count: 工作进程数
            heartbeat_timeout_sec: 心跳超时
            idle_ttl_sec: 空闲 TTL
            expose_http: 是否暴露 HTTP
            chunk_size: 上传分片大小
            healthy_only: 是否只使用健康节点
            tags: 节点标签过滤
            node_ids: 显式指定要部署到哪些节点
            node_count: 需要挑选的节点数量；未指定时默认使用 min_success_nodes
            node_limit: 节点数量限制
            allow_partial: 是否允许部分失败
            min_success_nodes: 最小成功节点数
            timeout_sec: 超时时间
            ensure_unique_service_name: 是否确保服务名唯一
            reuse_existing_same_code: 同 owner + 同代码时是否直接复用已存在服务
            replace_existing_if_code_changed: 同 owner + 同服务名但代码变化时是否替换（默认自动替换）
            session_cache_dir: 本地 service session token 缓存目录
            breaker_enabled: 是否启用熔断器
            breaker_failure_threshold: 熔断失败阈值
            breaker_cooldown_sec: 熔断冷却时间
            breaker_max_cooldown_sec: 熔断最大冷却时间

        Returns:
            ServiceGroup: 部署的服务组
        """
        entry_module = _normalize_entry_module_arg(entry_module)
        # 自动本地源码打包：处理模块对象和函数对象
        if module is not None:
            effective_blob, effective_filename = _prepare_code_blob(
                func=None,
                module=module,
                artifact_path="",
                blob=blob,
            )
            effective_package_format = "tar.gz"

            # 自动推断 entry_module
            if not entry_module:
                entry_module = _default_entry_module_for_module(module)
        elif func is not None:
            effective_blob, effective_filename = _prepare_code_blob(
                func=func,
                module=None,
                artifact_path="",
                blob=blob,
            )
            effective_package_format = "tar.gz"

            # 自动推断 entry_module 和 entry_callable
            if not entry_module:
                entry_module = _default_entry_module_for_func(func)
            if not entry_callable or entry_callable == "run":
                entry_callable = func.__name__
        else:
            effective_blob, effective_filename = _prepare_code_blob(
                func=None,
                module=None,
                artifact_path=artifact_path,
                blob=blob,
            )
            effective_package_format = package_format

        effective_package_format = _resolve_package_format(
            effective_package_format,
            effective_filename,
            default="py",
        )
        if effective_blob is not None and not effective_filename:
            effective_filename = _default_artifact_filename(
                package_format=effective_package_format,
                entry_module=entry_module,
                fallback_stem="service_artifact",
            )

        # 生成默认的 owner_client_id 和 service_name
        local_ip = _get_local_ip()

        # 如果 owner_client_id 为空，使用本机 IP
        effective_owner_client_id = owner_client_id
        if not effective_owner_client_id:
            effective_owner_client_id = f"client-{local_ip}"

        # 先确定 entry_module（用于生成 service_name）
        effective_entry_module = entry_module or _infer_entry_module_from_artifact_path(artifact_path)
        if not effective_entry_module:
            if effective_filename:
                # 优先使用推导出的 artifact 文件名
                if effective_filename.endswith(".py"):
                    effective_entry_module = Path(effective_filename).stem

        # 如果 service_name 为空，使用 entry_module + 本机 IP + 时间戳（精确到秒）
        # 添加时间戳确保唯一性，避免服务名冲突
        effective_service_name = service_name
        if not effective_service_name:
            # 生成时间戳（精确到秒）
            timestamp = time.strftime("%Y%m%d%H%M%S")  # 格式: 20250330120000

            if effective_entry_module:
                effective_service_name = f"{effective_entry_module}-{local_ip}-{timestamp}"
            else:
                effective_service_name = f"service-{local_ip}-{timestamp}"

        # 现在才进行校验
        if not effective_owner_client_id:
            raise ValueError("owner_client_id is required")
        if not effective_service_name:
            raise ValueError("service_name is required")

        if effective_blob is None:
            if not artifact_path:
                raise ValueError("artifact_path or blob must be provided")
            path = Path(artifact_path)
            if not path.exists():
                raise FileNotFoundError(f"artifact_path not found: {path}")
            effective_blob = path.read_bytes()
            if not effective_filename:
                effective_filename = path.name
                # 再次尝试从文件名推断 entry_module
                if not effective_entry_module and effective_filename.endswith(".py"):
                    effective_entry_module = Path(effective_filename).stem

        if effective_blob is None:
            raise ValueError("artifact content is empty")

        effective_code_version = _artifact_code_version(
            effective_blob,
            runtime=runtime,
            entry_module=effective_entry_module,
            entry_callable=entry_callable,
            package_format=effective_package_format,
            export_mode=export_mode,
            export_methods=export_methods,
            export_decorator=_DEFAULT_EXPORT_DECORATOR,
            dependency_allowlist=dependency_allowlist,
        )
        session_cache_file = _service_session_cache_file(
            owner_client_id=effective_owner_client_id,
            service_name=effective_service_name,
            cache_dir=session_cache_dir,
        )
        session_cache_lock: Optional[_ServiceSessionFileLock] = None

        requested_node_ids = [str(node_id).strip() for node_id in (node_ids or []) if str(node_id).strip()]
        requested_node_instance_ids = [str(node_id).strip() for node_id in (node_instance_ids or []) if str(node_id).strip()]
        desired_node_count = max(0, int(node_count or 0))
        required_success_nodes = max(1, int(min_success_nodes))
        discovery_limit = max(
            1,
            int(node_limit),
            len(requested_node_ids),
            len(requested_node_instance_ids),
            desired_node_count or required_success_nodes,
        )

        _emit_owner_notice(
            "deploy start "
            f"service_name={effective_service_name} owner={effective_owner_client_id} "
            f"target={infocenter_target} runtime={runtime} "
            f"requested_node_ids={requested_node_ids or 'auto'} "
            f"requested_node_instance_ids={requested_node_instance_ids or 'auto'} "
            f"min_success_nodes={required_success_nodes}"
        )

        def _discover_from_infocenter() -> Tuple[Sequence[InfoCenterServiceRoute], Sequence[InfoCenterNode]]:
            with InfoCenterClient(infocenter_target, timeout_sec=timeout_sec) as infocenter:
                existing_routes: Sequence[InfoCenterServiceRoute] = ()
                if ensure_unique_service_name:
                    existing_routes = infocenter.list_service_routes(
                        service_name=effective_service_name,
                        healthy_only=True,
                        limit=max(100, discovery_limit * 10),
                    )
                discovered_nodes = infocenter.list_nodes(
                    healthy_only=healthy_only,
                    tags=tags,
                    limit=discovery_limit,
                )
                return existing_routes, discovered_nodes

        existing_routes, discovered_nodes = _retry_infocenter_request(
            _discover_from_infocenter,
            timeout_sec=timeout_sec,
            target=infocenter_target,
            action="service deployment discovery",
        )

        if not discovered_nodes:
            _emit_owner_notice(
                "deploy failed: no available nodes "
                f"target={infocenter_target} healthy_only={healthy_only} tags={list(tags or ())}"
            )
            raise RuntimeError(
                f"no available nodes from InfoCenter: target={infocenter_target} "
                f"healthy_only={healthy_only} tags={list(tags or ())}"
            )

        normalized_runtime = normalize_python_runtime_spec(runtime)
        discovered_instance_map = {_node_instance_key_from_node(node): node for node in discovered_nodes}
        if requested_node_instance_ids:
            missing_node_instance_ids = [node_id for node_id in requested_node_instance_ids if node_id not in discovered_instance_map]
            if missing_node_instance_ids:
                raise RuntimeError(
                    f"requested node_instance_ids not found in current discovery scope: {missing_node_instance_ids}"
                )
            selected_nodes = [discovered_instance_map[node_id] for node_id in requested_node_instance_ids]
            if normalized_runtime:
                incompatible = [
                    _node_instance_key_from_node(node)
                    for node in selected_nodes
                    if str(node.python_version or "").strip()
                    and not matches_python_runtime(node.python_version, normalized_runtime)
                ]
                if incompatible:
                    raise RuntimeError(
                        f"requested node_instance_ids do not satisfy runtime {normalized_runtime}: {incompatible}"
                    )
        elif requested_node_ids:
            discovered_node_map = _build_unique_node_id_map(discovered_nodes, requested_ids=requested_node_ids)
            missing_node_ids = [node_id for node_id in requested_node_ids if node_id not in discovered_node_map]
            if missing_node_ids:
                raise RuntimeError(f"requested node_ids not found in current discovery scope: {missing_node_ids}")
            selected_nodes = [discovered_node_map[node_id] for node_id in requested_node_ids]
            if normalized_runtime:
                incompatible = [
                    node.node_id
                    for node in selected_nodes
                    if str(node.python_version or "").strip()
                    and not matches_python_runtime(node.python_version, normalized_runtime)
                ]
                if incompatible:
                    raise RuntimeError(
                        f"requested node_ids do not satisfy runtime {normalized_runtime}: {incompatible}"
                    )
        else:
            candidate_nodes = [
                node
                for node in discovered_nodes
                if node.healthy and node.schedulable and not node.drain
            ]
            if normalized_runtime:
                candidate_nodes = _filter_nodes_by_runtime(candidate_nodes, runtime=normalized_runtime)
            if not candidate_nodes:
                if normalized_runtime:
                    _emit_owner_notice(
                        "deploy failed: no schedulable nodes "
                        f"target={infocenter_target} runtime={normalized_runtime} "
                        f"candidates={_summarize_discovered_nodes(discovered_nodes)}"
                    )
                    raise RuntimeError(
                        f"no schedulable nodes from InfoCenter for runtime {normalized_runtime}; "
                        f"target={infocenter_target}; candidates={_summarize_discovered_nodes(discovered_nodes)}"
                    )
                _emit_owner_notice(
                    "deploy failed: no schedulable nodes "
                    f"target={infocenter_target} candidates={_summarize_discovered_nodes(discovered_nodes)}"
                )
                raise RuntimeError(
                    f"no schedulable nodes from InfoCenter; target={infocenter_target}; "
                    f"candidates={_summarize_discovered_nodes(discovered_nodes)}"
                )
            candidate_nodes.sort(
                key=lambda node: (
                    -int(node.service_worker_available),
                    -int(node.capacity),
                    int(node.queued),
                    node.node_id,
                )
            )
            effective_node_count = max(1, desired_node_count or required_success_nodes)
            selected_nodes = candidate_nodes[:effective_node_count]
            if len(selected_nodes) < required_success_nodes:
                raise RuntimeError(
                    "not enough schedulable nodes from InfoCenter: "
                    f"selected={len(selected_nodes)} required={required_success_nodes}"
                )

        if ensure_unique_service_name:
            active_routes = cls._select_active_routes(existing_routes)
            if active_routes:
                existing_infos = cls._inspect_existing_routes(active_routes=active_routes, timeout_sec=timeout_sec)
                existing_owners = {info.owner_client_id for _, info in existing_infos}
                existing_versions = {info.code_version for _, info in existing_infos}
                if len(existing_owners) != 1 or len(existing_versions) != 1:
                    raise RuntimeError(
                        f"service_name already exists but active routes are inconsistent: {effective_service_name}"
                    )

                existing_owner = next(iter(existing_owners))
                existing_code_version = next(iter(existing_versions))
                if existing_owner != effective_owner_client_id:
                    raise RuntimeError(
                        f"service_name already exists and belongs to another owner: "
                        f"service_name={effective_service_name}; owner={existing_owner}"
                    )

                cached_session = _load_service_session_cache(
                    owner_client_id=effective_owner_client_id,
                    service_name=effective_service_name,
                    cache_dir=session_cache_dir,
                )

                if existing_code_version == effective_code_version:
                    if not reuse_existing_same_code:
                        raise RuntimeError(
                            f"service_name already exists with same code_version: {effective_service_name}; "
                            "set reuse_existing_same_code=True to reuse"
                        )
                    if cached_session is None or cached_session.get("artifact_code_version") != effective_code_version:
                        raise RuntimeError(
                            f"service_name already exists with same code_version but no reusable local token cache was found: "
                            f"{effective_service_name}"
                        )
                    try:
                        session_cache_lock = _ServiceSessionFileLock(session_cache_file).acquire()
                    except RuntimeError as exc:
                        raise RuntimeError(
                            f"another local deploy process is already active for owner_client_id={effective_owner_client_id!r} "
                            f"service_name={effective_service_name!r}: {exc}"
                        ) from exc
                    group = cls._reuse_existing_group(
                        owner_client_id=effective_owner_client_id,
                        service_name=effective_service_name,
                        artifact_code_version=effective_code_version,
                        cache_payload=cached_session,
                        active_routes=existing_infos,
                        discovered_node_map=discovered_instance_map,
                        timeout_sec=timeout_sec,
                        breaker_enabled=breaker_enabled,
                        breaker_failure_threshold=breaker_failure_threshold,
                        breaker_cooldown_sec=breaker_cooldown_sec,
                        breaker_max_cooldown_sec=breaker_max_cooldown_sec,
                        session_cache_file=session_cache_file,
                        session_cache_lock=session_cache_lock,
                    )
                    _emit_owner_notice(
                        f"reuse existing service service_name={effective_service_name} nodes={list(group.sessions.keys())}"
                    )
                    return group

                raise RuntimeError(
                    f"service_name already exists with different code_version and is still running: "
                    f"{effective_service_name}; existing={existing_code_version}; incoming={effective_code_version}; "
                    "stop the active service first, then redeploy with the same service_name"
                )

        try:
            try:
                session_cache_lock = _ServiceSessionFileLock(session_cache_file).acquire()
            except RuntimeError as exc:
                raise RuntimeError(
                    f"another local deploy process is already active for owner_client_id={effective_owner_client_id!r} "
                    f"service_name={effective_service_name!r}: {exc}"
                ) from exc
            sessions: Dict[str, ServiceSessionClient] = {}
            clients: Dict[str, NodeControlClient] = {}
            nodes: Dict[str, InfoCenterNode] = {}
            failures: Dict[str, str] = {}

            for node in selected_nodes:
                client = NodeControlClient(node.control_addr, timeout_sec=timeout_sec)
                node_worker_count = max(1, int(worker_count or 1))
                if int(getattr(node, "service_worker_available", 0) or 0) > 0:
                    node_worker_count = max(1, min(node_worker_count, int(getattr(node, "service_worker_available", 0) or 0)))
                try:
                    session = client.create_service_from_bytes(
                        owner_client_id=effective_owner_client_id,
                        service_name=effective_service_name,
                        blob=effective_blob,
                        runtime=runtime,
                        entry_module=effective_entry_module,
                        entry_callable=entry_callable,
                        package_format=effective_package_format,
                        export_mode=export_mode,
                        export_methods=export_methods,
                        dependency_allowlist=dependency_allowlist,
                        managed_global_names=managed_global_names,
                        worker_count=node_worker_count,
                        heartbeat_timeout_sec=heartbeat_timeout_sec,
                        idle_ttl_sec=idle_ttl_sec,
                        expose_http=expose_http,
                        chunk_size=chunk_size,
                    )
                except Exception as exc:
                    failures[_node_instance_key_from_node(node)] = repr(exc)
                    client.close()
                    if not allow_partial:
                        cls._cleanup_created_services(sessions=sessions, clients=clients, reason="rollback deploy")
                        raise RuntimeError(
                            f"deploy failed on node={node.node_id}/{_node_instance_key_from_node(node)}: {exc}"
                        ) from exc
                    continue

                node_key = _node_instance_key_from_node(node)
                sessions[node_key] = session
                clients[node_key] = client
                nodes[node_key] = node

            if len(sessions) < required_success_nodes:
                cls._cleanup_created_services(sessions=sessions, clients=clients, reason="insufficient success nodes")
                _emit_owner_notice(
                    "deploy failed: insufficient success nodes "
                    f"service_name={effective_service_name} success={len(sessions)} "
                    f"required={required_success_nodes} failures={failures}"
                )
                raise RuntimeError(
                    f"deploy success nodes={len(sessions)} < min_success_nodes={required_success_nodes}; "
                    f"failures={failures}"
                )

            group = cls(
                owner_client_id=effective_owner_client_id,
                service_name=effective_service_name,
                sessions=sessions,
                nodes=nodes,
                failures=failures,
                breaker_enabled=bool(breaker_enabled),
                breaker_failure_threshold=max(1, int(breaker_failure_threshold)),
                breaker_cooldown_sec=max(0.1, float(breaker_cooldown_sec)),
                breaker_max_cooldown_sec=max(0.1, float(breaker_max_cooldown_sec)),
                _clients=clients,
                _session_cache_file=session_cache_file,
                _session_cache_lock=session_cache_lock,
                _artifact_code_version=effective_code_version,
            )
            group._persist_session_cache()
            group._start_keepalive()
            deployed_nodes = list(sessions.keys())
            if failures:
                _emit_owner_notice(
                    "deploy success with partial failures "
                    f"service_name={effective_service_name} nodes={deployed_nodes} failures={failures}"
                )
            else:
                _emit_owner_notice(
                    f"deploy success service_name={effective_service_name} nodes={deployed_nodes}"
                )
            return group
        except Exception:
            session_cache_lock.close()
            raise

    @classmethod
    def deploy_from_module(
        cls,
        *,
        infocenter_target: str,
        module: Any,
        owner_client_id: Optional[str] = None,
        service_name: Optional[str] = None,
        runtime: str = "py3",
        entry_callable: str = "run",
        export_mode: str = "decorator",
        export_methods: Optional[Sequence[str]] = None,
        dependency_allowlist: Optional[Sequence[str]] = None,
        managed_global_names: Optional[Sequence[str]] = None,
        worker_count: int = 10,
        heartbeat_timeout_sec: int = 30,
        idle_ttl_sec: int = 0,
        expose_http: bool = True,
        chunk_size: int = OBJECT_CHUNK_SIZE_BYTES,
        healthy_only: bool = True,
        tags: Optional[Sequence[str]] = None,
        node_ids: Optional[Sequence[str]] = None,
        node_instance_ids: Optional[Sequence[str]] = None,
        node_count: int = 0,
        node_limit: int = 100,
        allow_partial: bool = True,
        min_success_nodes: int = 1,
        timeout_sec: float = 10.0,
        ensure_unique_service_name: bool = True,
        reuse_existing_same_code: bool = True,
        replace_existing_if_code_changed: bool = True,
        session_cache_dir: str = "",
        breaker_enabled: bool = True,
        breaker_failure_threshold: int = 3,
        breaker_cooldown_sec: float = 15.0,
        breaker_max_cooldown_sec: float = 120.0,
    ) -> "ServiceGroup":
        return cls.deploy_from_infocenter(
            infocenter_target=infocenter_target,
            owner_client_id=owner_client_id,
            service_name=service_name,
            module=module,
            runtime=runtime,
            entry_callable=entry_callable,
            export_mode=export_mode,
            export_methods=export_methods,
            dependency_allowlist=dependency_allowlist,
            managed_global_names=managed_global_names,
            worker_count=worker_count,
            heartbeat_timeout_sec=heartbeat_timeout_sec,
            idle_ttl_sec=idle_ttl_sec,
            expose_http=expose_http,
            chunk_size=chunk_size,
            healthy_only=healthy_only,
            tags=tags,
            node_ids=node_ids,
            node_instance_ids=node_instance_ids,
            node_count=node_count,
            node_limit=node_limit,
            allow_partial=allow_partial,
            min_success_nodes=min_success_nodes,
            timeout_sec=timeout_sec,
            ensure_unique_service_name=ensure_unique_service_name,
            reuse_existing_same_code=reuse_existing_same_code,
            replace_existing_if_code_changed=replace_existing_if_code_changed,
            session_cache_dir=session_cache_dir,
            breaker_enabled=breaker_enabled,
            breaker_failure_threshold=breaker_failure_threshold,
            breaker_cooldown_sec=breaker_cooldown_sec,
            breaker_max_cooldown_sec=breaker_max_cooldown_sec,
        )

    @classmethod
    def deploy_from_func(
        cls,
        *,
        infocenter_target: str,
        func: Callable,
        owner_client_id: Optional[str] = None,
        service_name: Optional[str] = None,
        runtime: str = "py3",
        entry_module: Any = "",
        entry_callable: str = "run",
        export_mode: str = "decorator",
        export_methods: Optional[Sequence[str]] = None,
        dependency_allowlist: Optional[Sequence[str]] = None,
        managed_global_names: Optional[Sequence[str]] = None,
        worker_count: int = 10,
        heartbeat_timeout_sec: int = 30,
        idle_ttl_sec: int = 0,
        expose_http: bool = True,
        chunk_size: int = OBJECT_CHUNK_SIZE_BYTES,
        healthy_only: bool = True,
        tags: Optional[Sequence[str]] = None,
        node_ids: Optional[Sequence[str]] = None,
        node_instance_ids: Optional[Sequence[str]] = None,
        node_count: int = 0,
        node_limit: int = 100,
        allow_partial: bool = True,
        min_success_nodes: int = 1,
        timeout_sec: float = 10.0,
        ensure_unique_service_name: bool = True,
        reuse_existing_same_code: bool = True,
        replace_existing_if_code_changed: bool = True,
        session_cache_dir: str = "",
        breaker_enabled: bool = True,
        breaker_failure_threshold: int = 3,
        breaker_cooldown_sec: float = 15.0,
        breaker_max_cooldown_sec: float = 120.0,
    ) -> "ServiceGroup":
        return cls.deploy_from_infocenter(
            infocenter_target=infocenter_target,
            owner_client_id=owner_client_id,
            service_name=service_name,
            func=func,
            runtime=runtime,
            entry_module=entry_module,
            entry_callable=entry_callable,
            export_mode=export_mode,
            export_methods=export_methods,
            dependency_allowlist=dependency_allowlist,
            managed_global_names=managed_global_names,
            worker_count=worker_count,
            heartbeat_timeout_sec=heartbeat_timeout_sec,
            idle_ttl_sec=idle_ttl_sec,
            expose_http=expose_http,
            chunk_size=chunk_size,
            healthy_only=healthy_only,
            tags=tags,
            node_ids=node_ids,
            node_instance_ids=node_instance_ids,
            node_count=node_count,
            node_limit=node_limit,
            allow_partial=allow_partial,
            min_success_nodes=min_success_nodes,
            timeout_sec=timeout_sec,
            ensure_unique_service_name=ensure_unique_service_name,
            reuse_existing_same_code=reuse_existing_same_code,
            replace_existing_if_code_changed=replace_existing_if_code_changed,
            session_cache_dir=session_cache_dir,
            breaker_enabled=breaker_enabled,
            breaker_failure_threshold=breaker_failure_threshold,
            breaker_cooldown_sec=breaker_cooldown_sec,
            breaker_max_cooldown_sec=breaker_max_cooldown_sec,
        )

    @classmethod
    def deploy_from_file(
        cls,
        *,
        infocenter_target: str,
        artifact_path: str,
        owner_client_id: Optional[str] = None,
        service_name: Optional[str] = None,
        runtime: str = "py3",
        entry_module: Any = "",
        entry_callable: str = "run",
        package_format: str = "",
        export_mode: str = "decorator",
        export_methods: Optional[Sequence[str]] = None,
        dependency_allowlist: Optional[Sequence[str]] = None,
        managed_global_names: Optional[Sequence[str]] = None,
        worker_count: int = 10,
        heartbeat_timeout_sec: int = 30,
        idle_ttl_sec: int = 0,
        expose_http: bool = True,
        chunk_size: int = OBJECT_CHUNK_SIZE_BYTES,
        healthy_only: bool = True,
        tags: Optional[Sequence[str]] = None,
        node_ids: Optional[Sequence[str]] = None,
        node_instance_ids: Optional[Sequence[str]] = None,
        node_count: int = 0,
        node_limit: int = 100,
        allow_partial: bool = True,
        min_success_nodes: int = 1,
        timeout_sec: float = 10.0,
        ensure_unique_service_name: bool = True,
        reuse_existing_same_code: bool = True,
        replace_existing_if_code_changed: bool = True,
        session_cache_dir: str = "",
        breaker_enabled: bool = True,
        breaker_failure_threshold: int = 3,
        breaker_cooldown_sec: float = 15.0,
        breaker_max_cooldown_sec: float = 120.0,
    ) -> "ServiceGroup":
        return cls.deploy_from_infocenter(
            infocenter_target=infocenter_target,
            owner_client_id=owner_client_id,
            service_name=service_name,
            artifact_path=artifact_path,
            runtime=runtime,
            entry_module=entry_module,
            entry_callable=entry_callable,
            package_format=package_format,
            export_mode=export_mode,
            export_methods=export_methods,
            dependency_allowlist=dependency_allowlist,
            managed_global_names=managed_global_names,
            worker_count=worker_count,
            heartbeat_timeout_sec=heartbeat_timeout_sec,
            idle_ttl_sec=idle_ttl_sec,
            expose_http=expose_http,
            chunk_size=chunk_size,
            healthy_only=healthy_only,
            tags=tags,
            node_ids=node_ids,
            node_instance_ids=node_instance_ids,
            node_count=node_count,
            node_limit=node_limit,
            allow_partial=allow_partial,
            min_success_nodes=min_success_nodes,
            timeout_sec=timeout_sec,
            ensure_unique_service_name=ensure_unique_service_name,
            reuse_existing_same_code=reuse_existing_same_code,
            replace_existing_if_code_changed=replace_existing_if_code_changed,
            session_cache_dir=session_cache_dir,
            breaker_enabled=breaker_enabled,
            breaker_failure_threshold=breaker_failure_threshold,
            breaker_cooldown_sec=breaker_cooldown_sec,
            breaker_max_cooldown_sec=breaker_max_cooldown_sec,
        )

    @classmethod
    def deploy_from_bytes(
        cls,
        *,
        infocenter_target: str,
        blob: bytes,
        entry_module: Any = "",
        entry_callable: str = "run",
        owner_client_id: Optional[str] = None,
        service_name: Optional[str] = None,
        runtime: str = "py3",
        package_format: str = "py",
        export_mode: str = "decorator",
        export_methods: Optional[Sequence[str]] = None,
        dependency_allowlist: Optional[Sequence[str]] = None,
        managed_global_names: Optional[Sequence[str]] = None,
        worker_count: int = 10,
        heartbeat_timeout_sec: int = 30,
        idle_ttl_sec: int = 0,
        expose_http: bool = True,
        chunk_size: int = OBJECT_CHUNK_SIZE_BYTES,
        healthy_only: bool = True,
        tags: Optional[Sequence[str]] = None,
        node_ids: Optional[Sequence[str]] = None,
        node_instance_ids: Optional[Sequence[str]] = None,
        node_count: int = 0,
        node_limit: int = 100,
        allow_partial: bool = True,
        min_success_nodes: int = 1,
        timeout_sec: float = 10.0,
        ensure_unique_service_name: bool = True,
        reuse_existing_same_code: bool = True,
        replace_existing_if_code_changed: bool = True,
        session_cache_dir: str = "",
        breaker_enabled: bool = True,
        breaker_failure_threshold: int = 3,
        breaker_cooldown_sec: float = 15.0,
        breaker_max_cooldown_sec: float = 120.0,
    ) -> "ServiceGroup":
        return cls.deploy_from_infocenter(
            infocenter_target=infocenter_target,
            owner_client_id=owner_client_id,
            service_name=service_name,
            blob=blob,
            runtime=runtime,
            entry_module=entry_module,
            entry_callable=entry_callable,
            package_format=package_format,
            export_mode=export_mode,
            export_methods=export_methods,
            dependency_allowlist=dependency_allowlist,
            managed_global_names=managed_global_names,
            worker_count=worker_count,
            heartbeat_timeout_sec=heartbeat_timeout_sec,
            idle_ttl_sec=idle_ttl_sec,
            expose_http=expose_http,
            chunk_size=chunk_size,
            healthy_only=healthy_only,
            tags=tags,
            node_ids=node_ids,
            node_instance_ids=node_instance_ids,
            node_count=node_count,
            node_limit=node_limit,
            allow_partial=allow_partial,
            min_success_nodes=min_success_nodes,
            timeout_sec=timeout_sec,
            ensure_unique_service_name=ensure_unique_service_name,
            reuse_existing_same_code=reuse_existing_same_code,
            replace_existing_if_code_changed=replace_existing_if_code_changed,
            session_cache_dir=session_cache_dir,
            breaker_enabled=breaker_enabled,
            breaker_failure_threshold=breaker_failure_threshold,
            breaker_cooldown_sec=breaker_cooldown_sec,
            breaker_max_cooldown_sec=breaker_max_cooldown_sec,
        )

    @staticmethod
    def _select_active_routes(routes: Sequence[InfoCenterServiceRoute]) -> List[InfoCenterServiceRoute]:
        return [
            route
            for route in routes
            if route.status in (
                pb2.SERVICE_STATUS_STARTING,
                pb2.SERVICE_STATUS_RUNNING,
                pb2.SERVICE_STATUS_DRAINING,
            )
        ]

    @classmethod
    def _inspect_existing_routes(
        cls,
        *,
        active_routes: Sequence[InfoCenterServiceRoute],
        timeout_sec: float,
    ) -> List[Tuple[InfoCenterServiceRoute, pb2.ServiceStatusInfo]]:
        out: List[Tuple[InfoCenterServiceRoute, pb2.ServiceStatusInfo]] = []
        failures: Dict[str, str] = {}
        for route in active_routes:
            client = NodeControlClient(route.control_addr, timeout_sec=timeout_sec)
            try:
                info = client.get_service_status(service_id=route.service_id)
                out.append((route, info))
            except Exception as exc:
                failures[_node_instance_key_from_route(route)] = repr(exc)
            finally:
                client.close()
        if failures:
            raise RuntimeError(f"failed to inspect existing active service routes: {failures}")
        return out

    @classmethod
    def _reuse_existing_group(
        cls,
        *,
        owner_client_id: str,
        service_name: str,
        artifact_code_version: str,
        cache_payload: Dict[str, object],
        active_routes: Sequence[Tuple[InfoCenterServiceRoute, pb2.ServiceStatusInfo]],
        discovered_node_map: Dict[str, InfoCenterNode],
        timeout_sec: float,
        breaker_enabled: bool,
        breaker_failure_threshold: int,
        breaker_cooldown_sec: float,
        breaker_max_cooldown_sec: float,
        session_cache_file: Path,
        session_cache_lock: _ServiceSessionFileLock,
    ) -> "ServiceGroup":
        cache_nodes = cache_payload.get("nodes")
        if not isinstance(cache_nodes, dict):
            raise RuntimeError("invalid local service session cache: nodes missing")

        sessions: Dict[str, ServiceSessionClient] = {}
        clients: Dict[str, NodeControlClient] = {}
        nodes: Dict[str, InfoCenterNode] = {}

        try:
            for route, info in active_routes:
                route_key = _node_instance_key_from_route(route)
                node = discovered_node_map.get(route_key)
                if node is None:
                    raise RuntimeError(
                        f"existing service route is outside current discovery scope: node_instance_id={route_key}"
                    )

                cached_node = cache_nodes.get(route_key)
                if not isinstance(cached_node, dict):
                    raise RuntimeError(
                        f"local service session cache missing node entry for reuse: node_instance_id={route_key}"
                    )

                cached_service_id = str(cached_node.get("service_id", "")).strip()
                cached_token = str(cached_node.get("service_token", "")).strip()
                if cached_service_id != route.service_id:
                    raise RuntimeError(
                        f"local service session cache is stale for node_instance_id={route_key}: "
                        f"cached_service_id={cached_service_id} route_service_id={route.service_id}"
                    )
                if not cached_token:
                    raise RuntimeError(f"local service session cache missing token for node_instance_id={route_key}")

                client = NodeControlClient(route.control_addr, timeout_sec=timeout_sec)
                try:
                    hb = client.heartbeat_service(
                        owner_client_id=owner_client_id,
                        service_id=route.service_id,
                        service_token=cached_token,
                        seq=0,
                    )
                except Exception:
                    client.close()
                    raise

                sessions[route_key] = ServiceSessionClient(
                    _client=client,
                    owner_client_id=owner_client_id,
                    service_id=route.service_id,
                    service_token=cached_token,
                    http_base_url=str(cached_node.get("http_base_url", "") or info.http_base_url or route.http_base_url),
                    heartbeat_timeout_sec=max(
                        1,
                        int(
                            cached_node.get("heartbeat_timeout_sec", 0)
                            or (max(1, int(hb.next_heartbeat_in_sec or 0)) * 2)
                            or 30
                        ),
                    ),
                    worker_count=max(1, int(cached_node.get("worker_count", 0) or info.worker_count or route.worker_count or 1)),
                    status=hb.status or info.status,
                )
                clients[route_key] = client
                nodes[route_key] = node
        except Exception:
            for client in clients.values():
                try:
                    client.close()
                except Exception:
                    pass
            try:
                session_cache_lock.close()
            except Exception:
                pass
            raise

        group = cls(
            owner_client_id=owner_client_id,
            service_name=service_name,
            sessions=sessions,
            nodes=nodes,
            failures={},
            breaker_enabled=bool(breaker_enabled),
            breaker_failure_threshold=max(1, int(breaker_failure_threshold)),
            breaker_cooldown_sec=max(0.1, float(breaker_cooldown_sec)),
            breaker_max_cooldown_sec=max(0.1, float(breaker_max_cooldown_sec)),
            _clients=clients,
            _session_cache_file=session_cache_file,
            _session_cache_lock=session_cache_lock,
            _artifact_code_version=artifact_code_version,
        )
        group._persist_session_cache()
        group._start_keepalive()
        return group

    @classmethod
    def _end_existing_group(
        cls,
        *,
        owner_client_id: str,
        cache_payload: Dict[str, object],
        active_routes: Sequence[Tuple[InfoCenterServiceRoute, pb2.ServiceStatusInfo]],
        timeout_sec: float,
        reason: str,
    ) -> None:
        cache_nodes = cache_payload.get("nodes")
        if not isinstance(cache_nodes, dict):
            raise RuntimeError("invalid local service session cache: nodes missing")

        failures: Dict[str, str] = {}
        for route, _info in active_routes:
            route_key = _node_instance_key_from_route(route)
            cached_node = cache_nodes.get(route_key)
            if not isinstance(cached_node, dict):
                failures[route_key] = "missing cached node entry"
                continue
            cached_service_id = str(cached_node.get("service_id", "")).strip()
            cached_token = str(cached_node.get("service_token", "")).strip()
            if cached_service_id != route.service_id or not cached_token:
                failures[route_key] = "stale or missing cached token"
                continue

            client = NodeControlClient(route.control_addr, timeout_sec=timeout_sec)
            try:
                client.end_service(
                    owner_client_id=owner_client_id,
                    service_id=route.service_id,
                    service_token=cached_token,
                    reason=reason,
                )
            except Exception as exc:
                failures[route_key] = repr(exc)
            finally:
                client.close()

        if failures:
            raise RuntimeError(f"failed to end existing active service before replace: {failures}")

    @staticmethod
    def _cleanup_created_services(
        *,
        sessions: Dict[str, ServiceSessionClient],
        clients: Dict[str, NodeControlClient],
        reason: str,
    ) -> None:
        for session in sessions.values():
            try:
                session.end(reason)
            except Exception:
                pass
        for client in clients.values():
            try:
                client.close()
            except Exception:
                pass

    def _persist_session_cache(self) -> None:
        if self._session_cache_file is None or not self.sessions:
            return
        payload: Dict[str, object] = {
            "schema_version": _SERVICE_SESSION_SCHEMA_VERSION,
            "saved_at": datetime.now(timezone.utc).isoformat(),
            "owner_client_id": self.owner_client_id,
            "service_name": self.service_name,
            "artifact_code_version": self._artifact_code_version,
            "nodes": {},
        }
        nodes_payload: Dict[str, object] = {}
        for node_key, session in sorted(self.sessions.items()):
            node = self.nodes.get(node_key)
            control_addr = ""
            if node is not None:
                control_addr = node.control_addr
            elif node_key in self._clients:
                control_addr = self._clients[node_key].target
            nodes_payload[node_key] = {
                "node_id": str(node.node_id if node is not None else ""),
                "control_addr": control_addr,
                "service_id": session.service_id,
                "service_token": session.service_token,
                "http_base_url": session.http_base_url,
                "heartbeat_timeout_sec": int(session.heartbeat_timeout_sec),
                "worker_count": int(session.worker_count),
            }
        payload["nodes"] = nodes_payload
        if self._session_cache_lock is not None:
            self._session_cache_lock.write_json(payload)
        else:
            _write_private_json(self._session_cache_file, payload)

    def _clear_session_cache(self) -> None:
        if self._session_cache_file is None:
            return
        if self._session_cache_lock is not None:
            self._session_cache_lock.clear()
            self._delete_session_cache_on_close = True
            return
        try:
            self._session_cache_file.unlink()
        except FileNotFoundError:
            pass

    def __post_init__(self) -> None:
        if self.breaker_max_cooldown_sec < self.breaker_cooldown_sec:
            self.breaker_max_cooldown_sec = self.breaker_cooldown_sec
        for node_id in self.sessions:
            self._breaker_states.setdefault(node_id, NodeCircuitState())

    def _breaker_state_locked(self, node_id: str) -> NodeCircuitState:
        state = self._breaker_states.get(node_id)
        if state is None:
            state = NodeCircuitState()
            self._breaker_states[node_id] = state
        return state

    def _breaker_cooldown_locked(self, state: NodeCircuitState) -> float:
        exp = max(0, state.open_count - 1)
        cooldown = self.breaker_cooldown_sec * (2.0**exp)
        return min(self.breaker_max_cooldown_sec, cooldown)

    def _breaker_mark_success(self, node_id: str) -> None:
        if not self.breaker_enabled:
            return
        with self._route_lock:
            state = self._breaker_state_locked(node_id)
            state.state = "closed"
            state.consecutive_failures = 0
            state.open_until_monotonic = 0.0
            state.open_count = 0
            state.probe_in_flight = False
            state.last_error = ""

    def _breaker_mark_failure(self, node_id: str, exc: Exception) -> None:
        if not self.breaker_enabled:
            return
        now = time.monotonic()
        with self._route_lock:
            state = self._breaker_state_locked(node_id)
            state.last_error = repr(exc)
            if state.state == "half_open":
                state.consecutive_failures = max(state.consecutive_failures, self.breaker_failure_threshold)
            elif state.state == "closed":
                state.consecutive_failures += 1
            state.probe_in_flight = False

            if state.consecutive_failures < self.breaker_failure_threshold:
                return

            state.state = "open"
            state.open_count += 1
            state.open_until_monotonic = now + self._breaker_cooldown_locked(state)

    def _breaker_candidate_state(self, node_id: str) -> Tuple[str, bool]:
        if not self.breaker_enabled:
            return "closed", True
        now = time.monotonic()
        with self._route_lock:
            state = self._breaker_state_locked(node_id)
            if state.state == "open":
                if now >= state.open_until_monotonic:
                    state.state = "half_open"
                    state.probe_in_flight = False
                else:
                    return state.state, False
            if state.state == "half_open" and state.probe_in_flight:
                return state.state, False
            return state.state, True

    def _breaker_before_invoke(self, node_id: str) -> bool:
        if not self.breaker_enabled:
            return True
        now = time.monotonic()
        with self._route_lock:
            state = self._breaker_state_locked(node_id)
            if state.state == "open":
                if now < state.open_until_monotonic:
                    return False
                state.state = "half_open"
                state.probe_in_flight = False
            if state.state == "half_open":
                if state.probe_in_flight:
                    return False
                state.probe_in_flight = True
            return True

    def breaker_snapshot(self) -> Dict[str, Dict[str, object]]:
        now = time.monotonic()
        out: Dict[str, Dict[str, object]] = {}
        with self._route_lock:
            for node_id, state in self._breaker_states.items():
                remain = max(0.0, state.open_until_monotonic - now) if state.state == "open" else 0.0
                out[node_id] = {
                    "state": state.state,
                    "consecutive_failures": state.consecutive_failures,
                    "open_count": state.open_count,
                    "cooldown_remaining_sec": round(remain, 3),
                    "probe_in_flight": state.probe_in_flight,
                    "last_error": state.last_error,
                }
        return out

    def put_object_from_file(
        self,
        file_path: str,
        *,
        format: str = "",
        chunk_size: int = OBJECT_CHUNK_SIZE_BYTES,
    ) -> ObjectRef:
        refs = [
            client.upload_object_from_file(
                file_path=file_path,
                format=format,
                chunk_size=chunk_size,
            )
            for client in self._clients.values()
        ]
        if not refs:
            raise RuntimeError("no node clients available for object upload")
        object_ids = {ref.object_id for ref in refs}
        formats = {ref.format for ref in refs}
        if len(object_ids) != 1 or len(formats) != 1:
            raise RuntimeError(f"inconsistent object upload across nodes: {refs}")
        return refs[0]

    def put_object_from_bytes(
        self,
        blob: bytes,
        *,
        format: str = "",
        chunk_size: int = OBJECT_CHUNK_SIZE_BYTES,
    ) -> ObjectRef:
        refs = [
            client.upload_object_from_bytes(
                blob=blob,
                format=format,
                chunk_size=chunk_size,
            )
            for client in self._clients.values()
        ]
        if not refs:
            raise RuntimeError("no node clients available for object upload")
        object_ids = {ref.object_id for ref in refs}
        formats = {ref.format for ref in refs}
        if len(object_ids) != 1 or len(formats) != 1:
            raise RuntimeError(f"inconsistent object upload across nodes: {refs}")
        return refs[0]

    def put_data(
        self,
        data: Any,
        *,
        format: str = "",
        chunk_size: int = OBJECT_CHUNK_SIZE_BYTES,
    ) -> ObjectRef:
        return _put_data_via_clients(
            list(self._clients.values()),
            data,
            format=format,
            chunk_size=chunk_size,
        )

    def put_dataframe(self, dataframe: Any, *, chunk_size: int = OBJECT_CHUNK_SIZE_BYTES) -> ObjectRef:
        return self.put_data(dataframe, format="parquet", chunk_size=chunk_size)

    def put_ndarray(self, array: Any, *, chunk_size: int = OBJECT_CHUNK_SIZE_BYTES) -> ObjectRef:
        return self.put_data(array, format="npy", chunk_size=chunk_size)

    def put_json(self, value: Any, *, chunk_size: int = OBJECT_CHUNK_SIZE_BYTES) -> ObjectRef:
        return self.put_data(value, format="json", chunk_size=chunk_size)

    def update_globals(self, values: Dict[str, object]) -> str:
        with self._route_lock:
            sessions_snapshot = list(self.sessions.items())
            clients_snapshot = dict(self._clients)
        active_clients = [clients_snapshot[node_id] for node_id, _ in sessions_snapshot if node_id in clients_snapshot]
        prepared_values = _prepare_managed_globals_values_for_upload(active_clients, values)
        digests: Dict[str, str] = {}
        failed_nodes: Dict[str, str] = {}
        for node_id, session in sessions_snapshot:
            if getattr(session, "failed", False):
                failed_nodes[node_id] = str(getattr(session, "last_error", "") or "session failed")
                continue
            try:
                resp = session.update_globals_prepared(prepared_values)
                digests[node_id] = resp.globals_digest
            except Exception as exc:
                failed_nodes[node_id] = repr(exc)

        for node_id, message in failed_nodes.items():
            with self._route_lock:
                self.failures[node_id] = message
                self.sessions.pop(node_id, None)
                self.nodes.pop(node_id, None)
                client = self._clients.pop(node_id, None)
            if client is not None:
                try:
                    client.close()
                except Exception:
                    pass

        if not digests:
            raise RuntimeError(f"update_globals failed on all nodes: {failed_nodes}")
        self.globals_digests = dict(digests)
        unique = {digest for digest in digests.values() if str(digest).strip()}
        return next(iter(unique), "") if len(unique) == 1 else next(iter(digests.values()))

    def __enter__(self) -> "ServiceGroup":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close(end_services=False)

    def node_ids(self) -> Sequence[str]:
        return [self.nodes[key].node_id if key in self.nodes else key for key in self.sessions.keys()]

    def node_instance_ids(self) -> Sequence[str]:
        return list(self.sessions.keys())

    def _resolve_node_key(self, node_ref: str) -> str:
        normalized = str(node_ref or "").strip()
        if not normalized:
            raise KeyError("node reference is required")
        if normalized in self.sessions:
            return normalized
        matched = [node_key for node_key, node in self.nodes.items() if str(node.node_id or "").strip() == normalized]
        if len(matched) == 1:
            return matched[0]
        if len(matched) > 1:
            raise KeyError(
                f"ambiguous node_id: {normalized}; multiple live node instances match. Please use node_instance_id instead."
            )
        raise KeyError(f"unknown node reference: {normalized}")

    def _start_keepalive(self, interval_sec: Optional[float] = None) -> None:
        for session in self.sessions.values():
            session._start_keepalive(interval_sec=interval_sec)

    def join(
        self,
        *,
        poll_interval_sec: float = 1.0,
        end_services_on_interrupt: bool = True,
        end_reason: str = "owner interrupted",
    ) -> None:
        wait_sec = max(0.1, float(poll_interval_sec))
        try:
            while True:
                alive = False
                for session in self.sessions.values():
                    with session._hb_lock:
                        thread = session._hb_thread
                    if thread is not None and thread.is_alive():
                        alive = True
                        break
                if not alive:
                    self.failures = {
                        node_id: session.last_error
                        for node_id, session in self.sessions.items()
                        if getattr(session, "failed", False)
                    }
                    if self.failures:
                        _emit_owner_notice(
                            f"owner keepalive stopped service_name={self.service_name} failures={self.failures}"
                        )
                    return
                time.sleep(wait_sec)
        except KeyboardInterrupt:
            if end_services_on_interrupt:
                self.end(reason=end_reason)
            else:
                self._stop_keepalive()

    def _stop_keepalive(self) -> None:
        for session in self.sessions.values():
            session._stop_keepalive()

    def status_map(self) -> Dict[str, pb2.ServiceStatusInfo]:
        out: Dict[str, pb2.ServiceStatusInfo] = {}
        for node_key, session in self.sessions.items():
            out[node_key] = session.get_status()
        return out

    def call_on_node(
        self,
        node_id: str,
        method: str,
        payload: Dict[str, object],
        *,
        timeout_sec: float = 60.0,
    ) -> Dict[str, object]:
        node_key = self._resolve_node_key(node_id)
        session = self.sessions.get(node_key)
        if session is None:
            raise KeyError(f"unknown node reference: {node_id}")
        return session.call(method, payload, timeout_sec=timeout_sec)

    def _select_node(self, *, strategy: str, refresh_status: bool, exclude: Optional[Set[str]] = None) -> str:
        excluded = exclude or set()
        all_candidates = [nid for nid in sorted(self.sessions.keys()) if nid not in excluded]
        candidates = []
        state_rank: Dict[str, int] = {}
        for node_id in all_candidates:
            breaker_state, allowed = self._breaker_candidate_state(node_id)
            if not allowed:
                continue
            # Prefer closed nodes over half-open probe nodes.
            state_rank[node_id] = 0 if breaker_state == "closed" else 1
            candidates.append(node_id)
        if not candidates:
            raise RuntimeError("no available service node (all candidates may be open-circuit)")

        if strategy == "round_robin":
            ranked_candidates = sorted(candidates, key=lambda node_id: (state_rank.get(node_id, 0), node_id))
            with self._route_lock:
                idx = self._route_index % len(ranked_candidates)
                self._route_index += 1
            return ranked_candidates[idx]

        if strategy != "least_inflight":
            raise ValueError("strategy must be one of: least_inflight, round_robin")

        best_node_id = ""
        best_key: Optional[Tuple[int, int, int, str]] = None
        for node_id in candidates:
            session = self.sessions[node_id]
            info: Optional[pb2.ServiceStatusInfo] = None
            if refresh_status:
                try:
                    info = session.get_status()
                except Exception:
                    continue
                if info.status != pb2.SERVICE_STATUS_RUNNING:
                    continue
            in_flight = int(info.in_flight if info is not None else 0)
            alive_workers = int(info.alive_workers if info is not None else session.worker_count)
            key = (state_rank.get(node_id, 0), in_flight, -alive_workers, node_id)
            if best_key is None or key < best_key:
                best_key = key
                best_node_id = node_id

        if best_node_id:
            return best_node_id

        with self._route_lock:
            idx = self._route_index % len(candidates)
            self._route_index += 1
        return candidates[idx]

    def call_balanced(
        self,
        method: str,
        payload: Dict[str, object],
        *,
        timeout_sec: float = 60.0,
        strategy: str = "least_inflight",
        refresh_status: bool = True,
        max_attempts: int = 0,
    ) -> Tuple[str, Dict[str, object]]:
        if not self.sessions:
            raise RuntimeError("no active service sessions")

        # 序列化 Arrow 兼容对象
        serialized_payload = _serialize_arrow_compatible(payload)

        tries = max(1, int(max_attempts or len(self.sessions)))
        excluded: Set[str] = set()
        last_error: Optional[Exception] = None

        for _ in range(tries):
            node_id = self._select_node(strategy=strategy, refresh_status=refresh_status, exclude=excluded)
            excluded.add(node_id)
            if not self._breaker_before_invoke(node_id):
                continue
            try:
                resp = self.sessions[node_id].call(method, serialized_payload, timeout_sec=timeout_sec)
                self._breaker_mark_success(node_id)
                return node_id, resp
            except Exception as exc:
                last_error = exc
                self._breaker_mark_failure(node_id, exc)

        raise RuntimeError(f"call failed on all candidate nodes: {last_error}")

    async def acall_balanced(
        self,
        method: str,
        payload: Dict[str, object],
        *,
        timeout_sec: float = 60.0,
        strategy: str = "least_inflight",
        refresh_status: bool = True,
        max_attempts: int = 0,
    ) -> Tuple[str, Dict[str, object]]:
        """异步版本的 call_balanced。

        使用 asyncio 在线程池中执行同步 HTTP 调用，不阻塞事件循环。

        Args:
            method: 服务方法名
            payload: 调用参数
            timeout_sec: 超时时间
            strategy: 节点选择策略（"least_inflight" 或 "round_robin"）
            refresh_status: 是否在选择节点前刷新状态
            max_attempts: 最大尝试次数
        Returns:
            Tuple[str, Dict[str, object]]: (节点 ID, 响应结果)

        Raises:
            RuntimeError: 所有节点都调用失败时
        """
        if not self.sessions:
            raise RuntimeError("no active service sessions")

        tries = max(1, int(max_attempts or len(self.sessions)))
        excluded: Set[str] = set()
        last_error: Optional[Exception] = None

        loop = asyncio.get_running_loop()
        serialized_payload = _serialize_arrow_compatible(payload)
        for _ in range(tries):
            node_id = self._select_node(strategy=strategy, refresh_status=refresh_status, exclude=excluded)
            excluded.add(node_id)
            if not self._breaker_before_invoke(node_id):
                continue
            try:
                # 在线程池中执行同步调用，不阻塞事件循环
                resp = await loop.run_in_executor(
                    None,
                    lambda nid=node_id: self.sessions[nid].call(method, serialized_payload, timeout_sec=timeout_sec),
                )
                self._breaker_mark_success(node_id)
                return node_id, resp
            except Exception as exc:
                last_error = exc
                self._breaker_mark_failure(node_id, exc)

        raise RuntimeError(f"call failed on all candidate nodes: {last_error}")

    async def acall_all(
        self,
        method: str,
        payloads: Union[List[Dict[str, object]], Dict[str, object]],
        *,
        timeout_sec: float = 60.0,
        max_concurrency: int = 100,
    ) -> List[Tuple[Optional[str], Optional[Dict[str, object]], Optional[Exception]]]:
        """并发调用所有节点。

        将 payload 同时发送到所有可用节点，返回所有结果。

        Args:
            method: 服务方法名
            payloads: 可以是单个 payload（发送给所有节点）或 payload 列表（与节点一一对应）
            timeout_sec: 单次调用超时时间
            max_concurrency: 最大并发数

        Returns:
            List[Tuple[节点ID, 响应, 异常]]：所有节点的结果列表
        """
        if not self.sessions:
            raise RuntimeError("no active service sessions")

        nodes = list(self.sessions.keys())
        # 如果是单个 payload，复制给所有节点
        if isinstance(payloads, dict):
            shared_payload = _serialize_arrow_compatible(payloads)
            payloads = [dict(shared_payload) for _ in nodes]
        elif isinstance(payloads, list):
            if len(payloads) != len(nodes):
                raise ValueError(f"payload list length ({len(payloads)}) must match node count ({len(nodes)})")
            payloads = [_serialize_arrow_compatible(payload) for payload in payloads]

        loop = asyncio.get_running_loop()
        semaphore = asyncio.Semaphore(max_concurrency)

        async def _call_single(node_id: str, payload: Dict[str, object]) -> Tuple[Optional[str], Optional[Dict[str, object]], Optional[Exception]]:
            async with semaphore:
                if not self._breaker_before_invoke(node_id):
                    return node_id, None, RuntimeError("circuit breaker open")
                try:
                    resp = await loop.run_in_executor(
                        None,
                        lambda nid=node_id: self.sessions[nid].call(method, payload, timeout_sec=timeout_sec),
                    )
                    self._breaker_mark_success(node_id)
                    return node_id, resp, None
                except Exception as exc:
                    self._breaker_mark_failure(node_id, exc)
                    return node_id, None, exc

        tasks = [_call_single(node_id, payload) for node_id, payload in zip(nodes, payloads)]
        return await asyncio.gather(*tasks)

    def end(self, reason: str = "group end") -> Dict[str, Optional[pb2.EndServiceResponse]]:
        self._stop_keepalive()
        out: Dict[str, Optional[pb2.EndServiceResponse]] = {}
        for node_id, session in self.sessions.items():
            try:
                out[node_id] = session.end(reason)
            except Exception:
                out[node_id] = None
        if out and all(
            resp is not None and resp.ok and resp.accepted and resp.status == pb2.SERVICE_STATUS_STOPPED
            for resp in out.values()
        ):
            self._clear_session_cache()
        return out

    def close(self, *, end_services: bool = False, reason: str = "group close") -> None:
        self._stop_keepalive()
        if end_services:
            self.end(reason=reason)
        for client in self._clients.values():
            try:
                client.close()
            except Exception:
                pass
        self._clients.clear()
        if self._session_cache_lock is not None:
            self._session_cache_lock.close()
            self._session_cache_lock = None
        if self._delete_session_cache_on_close and self._session_cache_file is not None:
            try:
                self._session_cache_file.unlink()
            except FileNotFoundError:
                pass
            self._delete_session_cache_on_close = False


class _CallProxy:
    """服务方法调用代理。

    支持多种调用方式：
    - await proxy(x=1, y=2)  # 异步调用
    - proxy.sync(x=1, y=2)    # 同步调用
    - await proxy.broadcast(x=1)  # 广播到所有节点
    """

    def __init__(
        self,
        method: str,
        group: "ServiceGroup",
        *,
        timeout_sec: float = 60.0,
        strategy: str = "least_inflight",
        refresh_status: bool = True,
    ) -> None:
        self._method = method
        self._group = group
        self._timeout_sec = timeout_sec
        self._strategy = strategy
        self._refresh_status = refresh_status

    def __repr__(self) -> str:
        return f"<CallProxy method={self._method!r}>"

    @property
    def method(self) -> str:
        """返回方法名。"""
        return self._method

    async def __call__(self, *args, **kwargs) -> Dict[str, object]:
        """异步调用服务方法。

        Args:
            *args: 位置参数
            **kwargs: 命名参数

        Returns:
            Dict[str, object]: 服务的返回值

        Example:
            >>> # 命名参数
            >>> result = await group.square(x=7)
            >>> # 位置参数
            >>> result = await group.square(7)
            >>> # 混合使用
            >>> result = await group.compute(1, 2, c=3)
        """
        # 构造新的 payload 格式
        payload = {}
        if args:
            payload["args"] = list(args)
        if args and kwargs:
            payload["kwargs"] = kwargs

        if args:
            final_payload = payload
        else:
            final_payload = kwargs

        # 序列化 Arrow 兼容对象（DataFrame, Series, ndarray）
        serialized_payload = _serialize_arrow_compatible(final_payload)

        node_id, resp = await self._group.acall_balanced(
            self._method,
            serialized_payload,
            timeout_sec=self._timeout_sec,
            strategy=self._strategy,
            refresh_status=self._refresh_status,
        )
        return _resolve_high_level_service_data(self._group, node_id=node_id, response=resp)

    def __await__(self):
        """支持 await proxy() 语法。"""
        return self().__await__()

    @property
    def sync(self) -> "_SyncCallProxy":
        """返回同步调用代理。

        Example:
            >>> result = group.square.sync(x=7)
        """
        return _SyncCallProxy(
            method=self._method,
            group=self._group,
            timeout_sec=self._timeout_sec,
            strategy=self._strategy,
            refresh_status=self._refresh_status,
        )

    @property
    def broadcast(self) -> "_BroadcastProxy":
        """返回广播调用代理。

        Example:
            >>> results = await group.square.broadcast(x=7)
            >>> # results = [(node_id, result, error), ...]
        """
        return _BroadcastProxy(
            method=self._method,
            group=self._group,
            timeout_sec=self._timeout_sec,
        )

    def with_options(
        self,
        *,
        timeout_sec: Optional[float] = None,
        strategy: Optional[str] = None,
        refresh_status: Optional[bool] = None,
    ) -> "_CallProxy":
        """返回一个新的代理，使用指定的选项。

        Example:
            >>> proxy = group.square.with_options(timeout_sec=30)
            >>> result = await proxy(x=7)
        """
        return _CallProxy(
            method=self._method,
            group=self._group,
            timeout_sec=timeout_sec if timeout_sec is not None else self._timeout_sec,
            strategy=strategy if strategy is not None else self._strategy,
            refresh_status=refresh_status if refresh_status is not None else self._refresh_status,
        )


class _SyncCallProxy:
    """同步调用代理。"""

    def __init__(
        self,
        method: str,
        group: "ServiceGroup",
        *,
        timeout_sec: float = 60.0,
        strategy: str = "least_inflight",
        refresh_status: bool = True,
    ) -> None:
        self._method = method
        self._group = group
        self._timeout_sec = timeout_sec
        self._strategy = strategy
        self._refresh_status = refresh_status

    def __repr__(self) -> str:
        return f"<SyncCallProxy method={self._method!r}>"

    def __call__(self, *args, **kwargs) -> Dict[str, object]:
        """同步调用服务方法。

        Args:
            *args: 位置参数
            **kwargs: 命名参数

        Returns:
            Dict[str, object]: 服务的返回值

        Example:
            >>> # 命名参数
            >>> result = group.square.sync(x=7)
            >>> # 位置参数
            >>> result = group.square.sync(7)
            >>> # 混合使用
            >>> result = group.compute.sync(1, 2, c=3)
        """
        # 构造新的 payload 格式
        payload = {}
        if args:
            payload["args"] = list(args)
        if args and kwargs:
            payload["kwargs"] = kwargs

        if args:
            final_payload = payload
        else:
            final_payload = kwargs

        # 序列化 Arrow 兼容对象（DataFrame, Series, ndarray）
        serialized_payload = _serialize_arrow_compatible(final_payload)

        node_id, resp = self._group.call_balanced(
            self._method,
            serialized_payload,
            timeout_sec=self._timeout_sec,
            strategy=self._strategy,
            refresh_status=self._refresh_status,
        )
        return _resolve_high_level_service_data(self._group, node_id=node_id, response=resp)


class _BroadcastProxy:
    """广播调用代理，调用所有节点。"""

    def __init__(
        self,
        method: str,
        group: "ServiceGroup",
        *,
        timeout_sec: float = 60.0,
        max_concurrency: int = 100,
    ) -> None:
        self._method = method
        self._group = group
        self._timeout_sec = timeout_sec
        self._max_concurrency = max_concurrency

    def __repr__(self) -> str:
        return f"<BroadcastProxy method={self._method!r}>"

    async def __call__(
        self,
        **kwargs,
    ) -> List[Tuple[Optional[str], Optional[object], Optional[Exception]]]:
        """异步广播调用所有节点。

        Args:
            **kwargs: 方法参数

        Returns:
            List[Tuple[节点ID, 结果, 异常]]: 所有节点的结果

        Example:
            >>> results = await group.square.broadcast(x=7)
            >>> for node_id, result, error in results:
            ...     if error:
            ...         print(f"{node_id}: FAILED - {error}")
            ...     else:
            ...         print(f"{node_id}: {result}")
        """
        results = await self._group.acall_all(
            self._method,
            kwargs,
            timeout_sec=self._timeout_sec,
            max_concurrency=self._max_concurrency,
        )
        return _resolve_high_level_service_results(self._group, results=results)

    def __await__(self):
        return self().__await__()


class DeployedService(ServiceGroup):
    """模块化的服务组，像使用 Python 模块一样调用远程服务。

    支持多种调用方式：
    - await group.square(x=7)        # 异步调用
    - group.square.sync(x=7)         # 同步调用
    - await group.square.broadcast() # 广播到所有节点
    - group.list_methods()           # 列出所有可用方法

    Example:
        >>> group = DeployedService.deploy_from_infocenter(...)
        >>>
        >>> # 异步调用
        >>> result = await group.square(x=7)
        >>>
        >>> # 批量调用
        >>> results = await asyncio.gather(
        ...     group.square(x=i) for i in range(100)
        ... )
        >>>
        >>> # 同步调用
        >>> result = group.square.sync(x=7)
        >>>
        >>> # 广播调用
        >>> results = await group.square.broadcast(x=7)
    """

    # 缓存已发现的方法列表（使用普通属性，不是 dataclass field）
    _discovered_methods: Optional[List[str]] = None

    def __getattr__(self, name: str):
        """动态代理服务方法。

        Args:
            name: 方法名

        Returns:
            _CallProxy: 方法调用代理

        Raises:
            AttributeError: 如果方法不存在且已成功获取到非空方法列表
        """
        # 避免无限递归和处理特殊属性
        if name.startswith("_"):
            raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")

        # 如果还没有尝试过发现方法，先尝试发现
        if self._discovered_methods is None:
            self._ensure_methods_discovered()

        # 验证方法是否存在（空列表也应该验证）
        if self._discovered_methods is not None and name not in self._discovered_methods:
            raise AttributeError(
                f"'{type(self).__name__}' has no method '{name}'. "
                f"Available methods: {self._discovered_methods}"
            )

        return _CallProxy(
            method=name,
            group=self,
            timeout_sec=60.0,
            strategy="least_inflight",
            refresh_status=True,
        )

    def _ensure_methods_discovered(self) -> None:
        """确保方法列表已发现。"""
        if self._discovered_methods is not None:
            return

        # 尝试从 session 获取方法
        if self.sessions:
            first_session = next(iter(self.sessions.values()))
            try:
                methods = first_session.list_methods(include_docs=True)
                # ServiceMethodInfo 的字段名是 method
                self._discovered_methods = [m.method for m in methods]
                return
            except Exception:
                pass

        # 无法获取，设置为空列表
        self._discovered_methods = []

    def list_methods(self) -> List[str]:
        """列出所有可用的服务方法。

        Returns:
            List[str]: 方法名列表
        """
        self._ensure_methods_discovered()
        return list(self._discovered_methods or [])

    @property
    def methods(self) -> List[str]:
        """返回所有方法名的列表。

        Example:
            >>> print(group.methods)
            ['square', 'fibonacci', ...]
        """
        self._ensure_methods_discovered()
        return list(self._discovered_methods or [])

    async def call(self, method: str, **kwargs) -> Dict[str, object]:
        """通用异步调用接口。

        Args:
            method: 方法名
            **kwargs: 方法参数

        Returns:
            Dict[str, object]: 服务的返回值
        """
        node_id, resp = await self.acall_balanced(method, kwargs)
        return _resolve_high_level_service_data(self, node_id=node_id, response=resp)

    def call_sync(self, method: str, **kwargs) -> Dict[str, object]:
        """通用同步调用接口。

        Args:
            method: 方法名
            **kwargs: 方法参数

        Returns:
            Dict[str, object]: 服务的返回值
        """
        node_id, resp = self.call_balanced(method, kwargs)
        return _resolve_high_level_service_data(self, node_id=node_id, response=resp)

    async def call_all(
        self,
        method: str,
        **kwargs,
    ) -> List[Tuple[Optional[str], Optional[object], Optional[Exception]]]:
        """异步调用所有节点。

        Args:
            method: 方法名
            **kwargs: 方法参数

        Returns:
            List[Tuple[节点ID, 结果, 异常]]: 所有节点的结果
        """
        results = await self.acall_all(method, kwargs)
        return _resolve_high_level_service_results(self, results=results)

    def __repr__(self) -> str:
        node_ids = list(self.sessions.keys()) if self.sessions else []
        methods = self.methods if self._discovered_methods is not None else ["<not discovered>"]
        return (
            f"<DeployedService "
            f"service={self.service_name!r} "
            f"nodes={len(node_ids)} "
            f"methods={methods[:3]}{'...' if len(methods) > 3 else ''}>"
        )

class GatewayConnect(GatewayServiceClient):
    """Module-like caller on top of ControlPlane Gateway.

    只负责 caller 侧体验：
    - await client.square(x=7)
    - client.square.sync(x=7)
    - await client.call("square", x=7)
    - client.call_sync("square", x=7)

    不负责：
    - 上传代码
    - 创建服务
    - 心跳
    - EndService
    """

    def __init__(
        self,
        target: str,
        *,
        service_name: str,
        timeout_sec: float = 10.0,
        service_token: str = "",
        validate_on_init: bool = True,
    ) -> None:
        super().__init__(target, timeout_sec=timeout_sec, service_token=service_token)
        self.service_name = str(service_name or "").strip()
        if not self.service_name:
            raise ValueError("service_name is required")
        self._discovered_methods: Optional[List[str]] = None
        self._last_status: Optional[Dict[str, object]] = None
        if validate_on_init:
            self._validate_service_ready()

    def _validate_service_ready(self) -> Dict[str, object]:
        try:
            status = self.get_status(service_name=self.service_name)
        except Exception as exc:
            raise RuntimeError(
                f"failed to query gateway status for service_name={self.service_name!r} via {self.target}: {exc}"
            ) from exc
        if not isinstance(status, dict):
            raise RuntimeError(
                f"invalid gateway status for service_name={self.service_name!r} via {self.target}: {status!r}"
            )
        self._last_status = status
        route_count = int(status.get("route_count", 0) or 0)
        if route_count <= 0:
            raise RuntimeError(
                f"no available route for service_name={self.service_name!r} via gateway {self.target}"
            )
        return status

    def __getattr__(self, name: str):
        if name.startswith("_"):
            raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")
        if self._discovered_methods is None:
            self._ensure_methods_discovered()
        if self._discovered_methods is not None and name not in self._discovered_methods:
            raise AttributeError(
                f"'{type(self).__name__}' has no method '{name}'. "
                f"Available methods: {self._discovered_methods}"
            )
        return _CallProxy(
            method=name,
            group=self,
            timeout_sec=self.timeout_sec,
            strategy="gateway",
            refresh_status=False,
        )

    def _ensure_methods_discovered(self) -> None:
        if self._discovered_methods is not None:
            return
        try:
            methods = self.list_methods(include_docs=True)
        except Exception as exc:
            self._validate_service_ready()
            raise RuntimeError(
                f"failed to list methods for service_name={self.service_name!r} via gateway {self.target}: {exc}"
            ) from exc
        discovered = [str(item.get("method", "")).strip() for item in methods if str(item.get("method", "")).strip()]
        if not discovered:
            self._validate_service_ready()
            raise RuntimeError(
                f"service_name={self.service_name!r} has active gateway routes via {self.target} but no exported methods"
            )
        self._discovered_methods = discovered

    def refresh_methods(self) -> List[str]:
        self._discovered_methods = None
        self._ensure_methods_discovered()
        return list(self._discovered_methods or [])

    def list_methods(self, *, include_docs: bool = False) -> List[Dict[str, object]]:  # type: ignore[override]
        return list(super().list_methods(service_name=self.service_name, include_docs=include_docs))

    @property
    def methods(self) -> List[str]:
        self._ensure_methods_discovered()
        return list(self._discovered_methods or [])

    def status(self) -> Dict[str, object]:
        status = self.get_status(service_name=self.service_name)
        if isinstance(status, dict):
            self._last_status = status
        return status

    def call_balanced(
        self,
        method: str,
        payload: Dict[str, object],
        *,
        timeout_sec: float = 60.0,
        strategy: str = "gateway",
        refresh_status: bool = False,
        max_attempts: int = 0,
    ) -> Tuple[str, Dict[str, object]]:
        del strategy, refresh_status, max_attempts
        resp = super().call(
            service_name=self.service_name,
            method=method,
            payload=payload,
            timeout_sec=timeout_sec,
        )
        return "gateway", resp

    async def acall_balanced(
        self,
        method: str,
        payload: Dict[str, object],
        *,
        timeout_sec: float = 60.0,
        strategy: str = "gateway",
        refresh_status: bool = False,
        max_attempts: int = 0,
    ) -> Tuple[str, Dict[str, object]]:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None,
            lambda: self.call_balanced(
                method,
                payload,
                timeout_sec=timeout_sec,
                strategy=strategy,
                refresh_status=refresh_status,
                max_attempts=max_attempts,
            ),
        )

    async def call(self, method: str, **kwargs) -> Dict[str, object]:
        node_id, resp = await self.acall_balanced(method, kwargs, timeout_sec=self.timeout_sec)
        return _resolve_high_level_service_data(self, node_id=node_id, response=resp)

    def call_sync(self, method: str, **kwargs) -> Dict[str, object]:
        node_id, resp = self.call_balanced(method, kwargs, timeout_sec=self.timeout_sec)
        return _resolve_high_level_service_data(self, node_id=node_id, response=resp)

    async def acall_all(
        self,
        method: str,
        payload: Dict[str, object],
        *,
        timeout_sec: float = 60.0,
        max_concurrency: int = 100,
    ) -> List[Tuple[Optional[str], Optional[Dict[str, object]], Optional[Exception]]]:
        del method, payload, timeout_sec, max_concurrency
        raise NotImplementedError("GatewayConnect does not support broadcast; use Gateway for single-route calls")

    def __repr__(self) -> str:
        methods = self.methods if self._discovered_methods is not None else ["<not discovered>"]
        return (
            f"<GatewayConnect "
            f"service={self.service_name!r} "
            f"methods={methods[:3]}{'...' if len(methods) > 3 else ''}>"
        )


class DirectConnect(DiscoveryServiceClient):
    """Module-like caller built on InfoCenter discovery + direct instance calls."""

    def __init__(
        self,
        infocenter_target: str,
        *,
        service_name: str,
        timeout_sec: float = 10.0,
        service_token: str = "",
        refresh_interval_sec: float = 3.0,
        failure_threshold: int = 3,
        open_sec: float = 5.0,
        route_limit: int = 500,
        validate_on_init: bool = True,
    ) -> None:
        super().__init__(
            infocenter_target,
            timeout_sec=timeout_sec,
            service_token=service_token,
            refresh_interval_sec=refresh_interval_sec,
            failure_threshold=failure_threshold,
            open_sec=open_sec,
            route_limit=route_limit,
        )
        self.service_name = str(service_name or "").strip()
        if not self.service_name:
            raise ValueError("service_name is required")
        self._discovered_methods: Optional[List[str]] = None
        self._last_status: Optional[Dict[str, object]] = None
        if validate_on_init:
            self._validate_service_ready()

    def _validate_service_ready(self) -> Dict[str, object]:
        try:
            self.refresh_routes(service_name=self.service_name, force=True)
            status = self.get_status(service_name=self.service_name)
        except Exception as exc:
            raise RuntimeError(
                f"failed to query discovery status for service_name={self.service_name!r} via {self.infocenter_target}: {exc}"
            ) from exc
        if not isinstance(status, dict):
            raise RuntimeError(
                f"invalid discovery status for service_name={self.service_name!r} via {self.infocenter_target}: {status!r}"
            )
        self._last_status = status
        route_count = int(status.get("route_count", 0) or 0)
        if route_count <= 0:
            raise RuntimeError(
                f"no available route for service_name={self.service_name!r} via infocenter {self.infocenter_target}"
            )
        return status

    def __getattr__(self, name: str):
        if name.startswith("_"):
            raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")
        if self._discovered_methods is None:
            self._ensure_methods_discovered()
        if self._discovered_methods is not None and name not in self._discovered_methods:
            raise AttributeError(
                f"'{type(self).__name__}' has no method '{name}'. "
                f"Available methods: {self._discovered_methods}"
            )
        return _CallProxy(
            method=name,
            group=self,
            timeout_sec=self.timeout_sec,
            strategy="least_inflight",
            refresh_status=False,
        )

    def _ensure_methods_discovered(self) -> None:
        if self._discovered_methods is not None:
            return
        try:
            methods = self.list_methods(include_docs=True)
        except Exception as exc:
            self._validate_service_ready()
            raise RuntimeError(
                f"failed to list methods for service_name={self.service_name!r} via discovery {self.infocenter_target}: {exc}"
            ) from exc
        discovered = [str(item.get("method", "")).strip() for item in methods if str(item.get("method", "")).strip()]
        if not discovered:
            self._validate_service_ready()
            raise RuntimeError(
                f"service_name={self.service_name!r} has active discovery routes via {self.infocenter_target} but no exported methods"
            )
        self._discovered_methods = discovered

    def refresh_methods(self) -> List[str]:
        self._discovered_methods = None
        self._ensure_methods_discovered()
        return list(self._discovered_methods or [])

    def list_methods(self, *, include_docs: bool = False, strategy: str = "least_inflight") -> List[Dict[str, object]]:  # type: ignore[override]
        return list(
            super().list_methods(
                service_name=self.service_name,
                include_docs=include_docs,
                strategy=strategy,
            )
        )

    @property
    def methods(self) -> List[str]:
        self._ensure_methods_discovered()
        return list(self._discovered_methods or [])

    def status(self) -> Dict[str, object]:
        status = self.get_status(service_name=self.service_name)
        if isinstance(status, dict):
            self._last_status = status
        return status

    def call_balanced(
        self,
        method: str,
        payload: Dict[str, object],
        *,
        timeout_sec: float = 60.0,
        strategy: str = "least_inflight",
        refresh_status: bool = False,
        max_attempts: int = 0,
    ) -> Tuple[str, Dict[str, object]]:
        del refresh_status, max_attempts
        route = self._route_cache.select_route(self.service_name, strategy=strategy)
        tried = {route.service_id}
        token = self.service_token
        serialized_payload = _serialize_arrow_compatible(payload)
        try:
            resp = _call_route_http(
                route,
                method=method,
                payload=serialized_payload,
                timeout_sec=max(0.1, float(timeout_sec)),
                service_token=token,
            )
            self._route_cache.mark_success(route)
            return _node_instance_key_from_route(route), resp
        except DiscoveryCallError as exc:
            if not _is_route_failure(exc):
                raise RuntimeError(str(exc)) from exc
            self._route_cache.mark_failure(route, str(exc))
            self._route_cache.refresh(self.service_name, force=True)
            retry_route = self._route_cache.select_route(self.service_name, exclude_service_ids=tried, strategy=strategy)
            try:
                resp = _call_route_http(
                    retry_route,
                    method=method,
                    payload=serialized_payload,
                    timeout_sec=max(0.1, float(timeout_sec)),
                    service_token=token,
                )
                self._route_cache.mark_success(retry_route)
                return _node_instance_key_from_route(retry_route), resp
            except DiscoveryCallError as retry_exc:
                if _is_route_failure(retry_exc):
                    self._route_cache.mark_failure(retry_route, str(retry_exc))
                raise RuntimeError(str(retry_exc)) from retry_exc

    async def acall_balanced(
        self,
        method: str,
        payload: Dict[str, object],
        *,
        timeout_sec: float = 60.0,
        strategy: str = "least_inflight",
        refresh_status: bool = False,
        max_attempts: int = 0,
    ) -> Tuple[str, Dict[str, object]]:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None,
            lambda: self.call_balanced(
                method,
                payload,
                timeout_sec=timeout_sec,
                strategy=strategy,
                refresh_status=refresh_status,
                max_attempts=max_attempts,
            ),
        )

    async def call(self, method: str, **kwargs) -> Dict[str, object]:
        node_id, resp = await self.acall_balanced(method, kwargs, timeout_sec=self.timeout_sec)
        return _resolve_high_level_service_data(self, node_id=node_id, response=resp)

    def call_sync(self, method: str, **kwargs) -> Dict[str, object]:
        node_id, resp = self.call_balanced(method, kwargs, timeout_sec=self.timeout_sec)
        return _resolve_high_level_service_data(self, node_id=node_id, response=resp)

    async def acall_all(
        self,
        method: str,
        payload: Dict[str, object],
        *,
        timeout_sec: float = 60.0,
        max_concurrency: int = 100,
    ) -> List[Tuple[Optional[str], Optional[Dict[str, object]], Optional[Exception]]]:
        del method, payload, timeout_sec, max_concurrency
        raise NotImplementedError("DirectConnect does not support broadcast; use direct discovery for single-route calls")

    def __repr__(self) -> str:
        methods = self.methods if self._discovered_methods is not None else ["<not discovered>"]
        return (
            f"<DirectConnect "
            f"service={self.service_name!r} "
            f"methods={methods[:3]}{'...' if len(methods) > 3 else ''}>"
        )
