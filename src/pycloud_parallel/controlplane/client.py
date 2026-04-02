from __future__ import annotations

"""Client helpers for InfoCenter/NodeControl service-session workflow."""

import asyncio
import hashlib
import importlib
import inspect
import json
import os
import queue
import re
import socket
import sys
import tarfile
import tempfile
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Awaitable, Any, Callable, Dict, Iterator, List, Optional, Sequence, Set, Tuple, Union
from urllib.error import HTTPError
from urllib.parse import quote, urlencode, urlparse
from urllib.request import Request, urlopen

import grpc
from google.protobuf import timestamp_pb2

from pycloud_parallel.controlplane.runtime_spec import (
    matches_python_runtime,
    normalize_python_runtime_spec,
)
from pycloud_parallel.controlplane.serialization import dict_to_struct, serialize_arrow_compatible
from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2
from pycloud_parallel.grpc.v1 import pycloud_v1_pb2_grpc as pb2_grpc


def pycloud_export(fn):
    fn.__pycloud_export__ = True
    return fn


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


def _prepare_code_blob(
    func: Optional[Callable] = None,
    module: Optional[Any] = None,
    artifact_path: str = "",
    blob: Optional[bytes] = None,
) -> Tuple[Optional[bytes], str]:
    """准备代码 blob 和文件名。

    智能处理模块对象、函数对象、文件路径、直接 blob 四种情况。

    Args:
        func: 函数对象（自动打包依赖）
        module: 模块对象（自动打包整个模块）
        artifact_path: 文件路径
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

    # 优先级 4: 文件路径
    if artifact_path:
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

    # 打印 HTTP 请求信息
    url = f"{base_url.rstrip('/')}{path}"
    print(f"[HTTP {method.upper()}] {url}")
    if payload is not None:
        print(f"[Payload] {json.dumps(payload, ensure_ascii=False)}")
    print(f"[Headers] {request_headers}")

    req = Request(
        url,
        method=method.upper(),
        headers=request_headers,
        data=raw,
    )
    try:
        with urlopen(req, timeout=max(0.1, float(timeout_sec))) as resp:
            data = json.loads(resp.read().decode("utf-8") or "{}")
    except HTTPError as exc:
        try:
            body = json.loads((exc.read() or b"{}").decode("utf-8"))
        except Exception:
            body = {"ok": False, "error": exc.reason}
        raise RuntimeError(str(body.get("error", exc.reason))) from exc
    if not isinstance(data, dict):
        raise RuntimeError("invalid json response")
    if data.get("ok", False) is False:
        raise RuntimeError(str(data.get("error", "request failed")))
    return data


def _sha256_file(path: Path, *, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fp:
        while True:
            chunk = fp.read(max(1, int(chunk_size)))
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def _iter_file_chunks(path: Path, *, chunk_size: int = 256 * 1024) -> Iterator[bytes]:
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


def _build_export_spec(
    *,
    export_mode: str,
    export_methods: Optional[Sequence[str]],
    export_decorator: str,
) -> pb2.ModuleExportSpec:
    return pb2.ModuleExportSpec(
        mode=str(export_mode or "").strip(),
        methods=[x.strip() for x in (export_methods or []) if str(x).strip()],
        decorator=str(export_decorator or "").strip(),
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


_SERVICE_SESSION_SCHEMA_VERSION = 1


def _artifact_code_version(blob: bytes) -> str:
    return f"sha256:{hashlib.sha256(blob).hexdigest()}"


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
class InfoCenterNode:
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


@dataclass(frozen=True)
class InfoCenterServiceRoute:
    service_name: str
    service_id: str
    status: int
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
        healthy: bool = True,
        metrics: Optional[Dict[str, object]] = None,
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
                "healthy": bool(healthy),
                "metrics": dict(metrics or {}),
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
            out.append(
                InfoCenterNode(
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
        node_count: int = 0,
        limit: int = 100,
        require_credit: bool = True,
        preferred_runtime_key: str = "",
        runtime: str = "",
    ) -> Sequence[InfoCenterNode]:
        nodes = list(self.list_nodes(healthy_only=healthy_only, tags=tags, limit=limit))
        requested_node_ids = [str(node_id).strip() for node_id in (node_ids or []) if str(node_id).strip()]
        preferred_runtime = str(preferred_runtime_key or "").strip()
        normalized_runtime = normalize_python_runtime_spec(runtime)
        discovered_node_map = {node.node_id: node for node in nodes}

        if requested_node_ids:
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
        return _http_json_request(
            base_url=self.base_url,
            path=f"/svc/{quote(name, safe='')}/call/{quote(method_name, safe='')}?{params}",
            method="POST",
            timeout_sec=max(self.timeout_sec, max(0.1, float(timeout_sec)) + 1.0),
            payload=payload or {},
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
            candidates.sort(key=lambda route: (route.node_id, route.service_id))
            with self._lock:
                idx = self._route_index.get(name, 0)
                self._route_index[name] = idx + 1
            return candidates[idx % len(candidates)]
        if strategy != "least_inflight":
            raise ValueError("strategy must be one of: least_inflight, round_robin")
        candidates.sort(key=lambda route: (int(route.in_flight), -int(route.alive_workers), route.node_id, route.service_id))
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
    serialized_payload = _serialize_arrow_compatible(payload or {})
    req = Request(
        url=url,
        method="POST",
        headers=headers,
        data=json.dumps(serialized_payload).encode("utf-8"),
    )
    try:
        with urlopen(req, timeout=max(2.0, timeout_sec + 1.0)) as resp:
            data = json.loads(resp.read().decode("utf-8") or "{}")
    except HTTPError as exc:
        try:
            data = json.loads((exc.read() or b"{}").decode("utf-8"))
        except Exception:
            data = {"ok": False, "error": exc.reason}
        raise DiscoveryCallError(status_code=exc.code, data=data) from exc
    except Exception as exc:
        raise DiscoveryCallError(status_code=502, data={"ok": False, "error": repr(exc)}) from exc
    if not isinstance(data, dict):
        raise DiscoveryCallError(status_code=502, data={"ok": False, "error": "invalid json response"})
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
    _hb_stop: threading.Event = field(default_factory=threading.Event, repr=False)
    _hb_thread: Optional[threading.Thread] = field(default=None, repr=False)
    _hb_lock: threading.Lock = field(default_factory=threading.Lock, repr=False)
    _hb_seq: int = field(default=0, repr=False)
    _hb_interval_sec: float = field(default=0.0, repr=False)

    def _start_keepalive(self, interval_sec: Optional[float] = None) -> None:
        with self._hb_lock:
            if self._hb_thread is not None and self._hb_thread.is_alive():
                return
            self._hb_stop.clear()
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
        self._hb_seq += 1
        resp = self._client.heartbeat_service(
            owner_client_id=self.owner_client_id,
            service_id=self.service_id,
            service_token=self.service_token,
            seq=self._hb_seq,
        )
        self.status = resp.status
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
        serialized_payload = _serialize_arrow_compatible(payload or {})
        req = Request(
            url=url,
            method="POST",
            headers=headers,
            data=json.dumps(serialized_payload).encode("utf-8"),
        )
        try:
            with urlopen(req, timeout=max(2.0, float(timeout_sec) + 1.0)) as resp:
                body = json.loads(resp.read().decode("utf-8"))
        except HTTPError as exc:
            raw = exc.read().decode("utf-8") if hasattr(exc, "read") else ""
            msg = raw or str(exc)
            raise RuntimeError(f"call failed: {msg}") from exc
        if not isinstance(body, dict):
            raise RuntimeError("call failed: invalid response body")
        if not body.get("ok", False):
            raise RuntimeError(f"call failed: {body.get('error', 'unknown error')}")
        return body

    def _keepalive_loop(self) -> None:
        while not self._hb_stop.wait(max(0.5, self._hb_interval_sec)):
            try:
                self.heartbeat()
            except Exception:
                # Keep trying. If heartbeat stays broken for too long, server will reclaim service.
                self._hb_interval_sec = max(1.0, self._hb_interval_sec)


class NodeControlClient:
    """Thin gRPC client wrapper for NodeControl service."""

    def __init__(self, target: str, *, timeout_sec: float = 10.0) -> None:
        self.target = target
        self.timeout_sec = max(0.1, float(timeout_sec))
        self.channel = grpc.insecure_channel(target)
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
        filename: str = "",
        runtime: str = "py3",
        entry_module: str = "",
        entry_callable: str = "run",
        package_format: str = "",
        export_mode: str = "single",
        export_methods: Optional[Sequence[str]] = None,
        export_decorator: str = "pycloud_export",
        dependency_allowlist: Optional[Sequence[str]] = None,
        chunk_size: int = 256 * 1024,
    ) -> pb2.UploadCodeResponse:
        path = Path(artifact_path)
        if not path.exists():
            raise FileNotFoundError(f"artifact_path not found: {artifact_path}")

        tmp_pkg: Optional[Path] = None
        upload_file = path
        effective_filename = filename or path.name
        inferred_format = package_format
        if path.is_dir():
            tmp_pkg = _package_directory_to_targz(path)
            upload_file = tmp_pkg
            effective_filename = filename or f"{path.name}.tar.gz"
            inferred_format = package_format or "tar.gz"

        try:
            return self._upload_code_from_local_file(
                client_id=client_id,
                file_path=upload_file,
                filename=effective_filename,
                runtime=runtime,
                entry_module=entry_module,
                entry_callable=entry_callable,
                package_format=inferred_format,
                export_mode=export_mode,
                export_methods=export_methods,
                export_decorator=export_decorator,
                dependency_allowlist=dependency_allowlist,
                chunk_size=chunk_size,
            )
        finally:
            if tmp_pkg is not None:
                tmp_pkg.unlink(missing_ok=True)

    def upload_code_from_bytes(
        self,
        *,
        client_id: str,
        filename: str,
        blob: bytes,
        runtime: str = "py3",
        entry_module: str = "",
        entry_callable: str = "run",
        package_format: str = "",
        export_mode: str = "single",
        export_methods: Optional[Sequence[str]] = None,
        export_decorator: str = "pycloud_export",
        dependency_allowlist: Optional[Sequence[str]] = None,
        chunk_size: int = 256 * 1024,
    ) -> pb2.UploadCodeResponse:
        if not client_id:
            raise ValueError("client_id is required")
        if not filename:
            raise ValueError("filename is required")

        effective_format = package_format or _package_format_from_filename(filename)
        effective_module = entry_module or (Path(filename).stem if filename.endswith(".py") else "")
        export_spec = _build_export_spec(
            export_mode=export_mode,
            export_methods=export_methods,
            export_decorator=export_decorator,
        )
        digest = hashlib.sha256(blob).hexdigest()

        def _iter() -> Iterator[pb2.UploadCodeRequest]:
            yield pb2.UploadCodeRequest(
                meta=pb2.UploadCodeMeta(
                    client_id=client_id,
                    filename=filename,
                    sha256=f"sha256:{digest}",
                    runtime=runtime,
                    entry_module=effective_module,
                    entry_callable=entry_callable or "run",
                    package_format=effective_format,
                    export_spec=export_spec,
                    dependency_allowlist=list(dependency_allowlist or ()),
                )
            )
            for i in range(0, len(blob), max(1, int(chunk_size))):
                yield pb2.UploadCodeRequest(chunk=blob[i : i + chunk_size])

        resp = self.stub.UploadCode(_iter(), timeout=self.timeout_sec)
        if not resp.ok:
            raise RuntimeError(_err_msg(resp.error, "upload code failed"))
        return resp

    def _upload_code_from_local_file(
        self,
        *,
        client_id: str,
        file_path: Path,
        filename: str,
        runtime: str,
        entry_module: str,
        entry_callable: str,
        package_format: str,
        export_mode: str,
        export_methods: Optional[Sequence[str]],
        export_decorator: str,
        dependency_allowlist: Optional[Sequence[str]],
        chunk_size: int,
    ) -> pb2.UploadCodeResponse:
        effective_filename = filename or file_path.name
        effective_module = entry_module or (Path(effective_filename).stem if effective_filename.endswith(".py") else "")
        effective_format = package_format or _package_format_from_filename(effective_filename)
        export_spec = _build_export_spec(
            export_mode=export_mode,
            export_methods=export_methods,
            export_decorator=export_decorator,
        )
        digest = _sha256_file(file_path)

        def _iter() -> Iterator[pb2.UploadCodeRequest]:
            yield pb2.UploadCodeRequest(
                meta=pb2.UploadCodeMeta(
                    client_id=client_id,
                    filename=effective_filename,
                    sha256=f"sha256:{digest}",
                    runtime=runtime,
                    entry_module=effective_module,
                    entry_callable=entry_callable or "run",
                    package_format=effective_format,
                    export_spec=export_spec,
                    dependency_allowlist=list(dependency_allowlist or ()),
                )
            )
            yield from (pb2.UploadCodeRequest(chunk=chunk) for chunk in _iter_file_chunks(file_path, chunk_size=chunk_size))

        resp = self.stub.UploadCode(_iter(), timeout=self.timeout_sec)
        if not resp.ok:
            raise RuntimeError(_err_msg(resp.error, "upload code failed"))
        return resp

    def submit_tasks(
        self,
        *,
        client_id: str,
        code_version: str,
        tasks: Sequence[pb2.TaskSubmitItem],
        execution_mode: int = pb2.EXECUTION_MODE_PERSISTENT,
        job_id: str = "",
    ) -> pb2.SubmitTasksResponse:
        resp = self.stub.SubmitTasks(
            pb2.SubmitTasksRequest(
                client_id=client_id,
                code_version=code_version,
                execution_mode=execution_mode,
                tasks=list(tasks),
                job_id=job_id,
            ),
            timeout=self.timeout_sec,
        )
        if not resp.ok:
            raise RuntimeError(_err_msg(resp.error, "submit tasks failed"))
        return resp

    def pull_results(
        self,
        *,
        client_id: str,
        limit: int = 100,
        wait_ms: int = 0,
        cursor: str = "",
    ) -> pb2.PullResultsResponse:
        resp = self.stub.PullResults(
            pb2.PullResultsRequest(
                client_id=client_id,
                limit=max(1, int(limit)),
                wait_ms=max(0, int(wait_ms)),
                cursor=cursor,
            ),
            timeout=max(self.timeout_sec, max(0.1, float(wait_ms) / 1000.0) + 1.0),
        )
        if not resp.ok:
            raise RuntimeError(_err_msg(resp.error, "pull results failed"))
        return resp

    def cancel_tasks(
        self,
        *,
        client_id: str,
        task_ids: Sequence[str],
        reason: str = "",
    ) -> pb2.CancelTasksResponse:
        resp = self.stub.CancelTasks(
            pb2.CancelTasksRequest(
                client_id=client_id,
                task_ids=[str(task_id) for task_id in task_ids],
                reason=reason,
            ),
            timeout=self.timeout_sec,
        )
        if not resp.ok:
            raise RuntimeError(_err_msg(resp.error, "cancel tasks failed"))
        return resp

    def cancel_job(
        self,
        *,
        client_id: str,
        job_id: str,
        reason: str = "",
    ) -> pb2.CancelJobResponse:
        resp = self.stub.CancelJob(
            pb2.CancelJobRequest(
                client_id=client_id,
                job_id=job_id,
                reason=reason,
            ),
            timeout=self.timeout_sec,
        )
        if not resp.ok:
            raise RuntimeError(_err_msg(resp.error, "cancel job failed"))
        return resp

    def get_metrics(self) -> pb2.GetMetricsResponse:
        resp = self.stub.GetMetrics(
            pb2.GetMetricsRequest(),
            timeout=self.timeout_sec,
        )
        if not resp.ok:
            raise RuntimeError(_err_msg(resp.error, "get metrics failed"))
        return resp

    def open_task_stream(
        self,
        *,
        client_id: str,
        code_version: str,
        result_limit: int = 100,
        result_wait_ms: int = 200,
    ) -> "TaskStreamSession":
        return TaskStreamSession(
            _client=self,
            client_id=client_id,
            code_version=code_version,
            result_limit=result_limit,
            result_wait_ms=result_wait_ms,
        )

    def create_service_from_file(
        self,
        *,
        owner_client_id: str,
        artifact_path: str,
        service_name: str = "",
        filename: str = "",
        runtime: str = "py3",
        entry_module: str = "",
        entry_callable: str = "run",
        package_format: str = "",
        export_mode: str = "decorator",
        export_methods: Optional[Sequence[str]] = None,
        export_decorator: str = "pycloud_export",
        dependency_allowlist: Optional[Sequence[str]] = None,
        worker_count: int = 10,
        heartbeat_timeout_sec: int = 30,
        idle_ttl_sec: int = 0,
        expose_http: bool = True,
        chunk_size: int = 256 * 1024,
    ) -> ServiceSessionClient:
        path = Path(artifact_path)
        if not path.exists():
            raise FileNotFoundError(f"artifact_path not found: {artifact_path}")

        tmp_pkg: Optional[Path] = None
        upload_file = path
        effective_filename = filename or path.name
        inferred_format = package_format
        if path.is_dir():
            tmp_pkg = _package_directory_to_targz(path)
            upload_file = tmp_pkg
            effective_filename = filename or f"{path.name}.tar.gz"
            inferred_format = package_format or "tar.gz"

        try:
            return self._create_service_from_local_file(
                owner_client_id=owner_client_id,
                service_name=service_name,
                file_path=upload_file,
                filename=effective_filename,
                runtime=runtime,
                entry_module=entry_module,
                entry_callable=entry_callable,
                package_format=inferred_format,
                export_mode=export_mode,
                export_methods=export_methods,
                export_decorator=export_decorator,
                dependency_allowlist=dependency_allowlist,
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
        filename: str = "service_bundle.tar.gz",
        runtime: str = "py3",
        entry_module: str = "",
        entry_callable: str = "run",
        export_mode: str = "decorator",
        export_methods: Optional[Sequence[str]] = None,
        export_decorator: str = "pycloud_export",
        dependency_allowlist: Optional[Sequence[str]] = None,
        worker_count: int = 10,
        heartbeat_timeout_sec: int = 30,
        idle_ttl_sec: int = 0,
        expose_http: bool = True,
        chunk_size: int = 256 * 1024,
    ) -> ServiceSessionClient:
        tar_path = _package_paths_to_targz(root_dir=Path(root_dir), paths=paths)
        try:
            return self._create_service_from_local_file(
                owner_client_id=owner_client_id,
                service_name=service_name,
                file_path=tar_path,
                filename=filename or "service_bundle.tar.gz",
                runtime=runtime,
                entry_module=entry_module,
                entry_callable=entry_callable,
                package_format="tar.gz",
                export_mode=export_mode,
                export_methods=export_methods,
                export_decorator=export_decorator,
                dependency_allowlist=dependency_allowlist,
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
        filename: str,
        runtime: str,
        entry_module: str,
        entry_callable: str,
        package_format: str,
        export_mode: str,
        export_methods: Optional[Sequence[str]],
        export_decorator: str,
        dependency_allowlist: Optional[Sequence[str]],
        worker_count: int,
        heartbeat_timeout_sec: int,
        idle_ttl_sec: int,
        expose_http: bool,
        chunk_size: int,
    ) -> ServiceSessionClient:
        effective_filename = filename or file_path.name
        effective_module = entry_module
        if not effective_module and effective_filename.endswith(".py"):
            effective_module = Path(effective_filename).stem
        effective_format = package_format or _package_format_from_filename(effective_filename)
        digest = _sha256_file(file_path)
        export_spec = _build_export_spec(
            export_mode=export_mode,
            export_methods=export_methods,
            export_decorator=export_decorator,
        )

        def _iter() -> Iterator[pb2.CreateServiceRequest]:
            yield pb2.CreateServiceRequest(
                meta=pb2.CreateServiceMeta(
                    owner_client_id=owner_client_id,
                    service_name=service_name,
                    filename=effective_filename,
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
        filename: str,
        blob: bytes,
        runtime: str,
        entry_module: str,
        entry_callable: str,
        package_format: str = "",
        export_mode: str = "decorator",
        export_methods: Optional[Sequence[str]] = None,
        export_decorator: str = "pycloud_export",
        dependency_allowlist: Optional[Sequence[str]] = None,
        worker_count: int = 10,
        heartbeat_timeout_sec: int = 30,
        idle_ttl_sec: int = 0,
        expose_http: bool = True,
        chunk_size: int = 256 * 1024,
    ) -> ServiceSessionClient:
        if not owner_client_id:
            raise ValueError("owner_client_id is required")
        if not filename:
            raise ValueError("filename is required")

        digest = hashlib.sha256(blob).hexdigest()
        effective_format = package_format or _package_format_from_filename(filename)
        export_spec = _build_export_spec(
            export_mode=export_mode,
            export_methods=export_methods,
            export_decorator=export_decorator,
        )

        def _iter() -> Iterator[pb2.CreateServiceRequest]:
            yield pb2.CreateServiceRequest(
                meta=pb2.CreateServiceMeta(
                    owner_client_id=owner_client_id,
                    service_name=service_name,
                    filename=filename,
                    sha256=f"sha256:{digest}",
                    runtime=runtime,
                    entry_module=entry_module,
                    entry_callable=entry_callable or "run",
                    worker_count=max(1, int(worker_count)),
                    heartbeat_timeout_sec=max(1, int(heartbeat_timeout_sec)),
                    idle_ttl_sec=max(0, int(idle_ttl_sec)),
                    expose_http=bool(expose_http),
                    package_format=effective_format,
                    export_spec=export_spec,
                    dependency_allowlist=list(dependency_allowlist or ()),
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
        resp = self.stub.CallService(
            pb2.CallServiceRequest(
                service_id=service_id,
                method=method,
                payload=dict_to_struct(_serialize_arrow_compatible(payload or {})),
                timeout_sec=max(0.1, float(timeout_sec)),
                service_token=service_token or "",
            ),
            timeout=max(self.timeout_sec, max(0.1, float(timeout_sec)) + 1.0),
        )
        if not resp.ok:
            reason = resp.task_error.message if resp.task_error and resp.task_error.message else _err_msg(resp.error, "call service failed")
            raise RuntimeError(reason)
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
class TaskStreamSession:
    _client: NodeControlClient = field(repr=False)
    client_id: str
    code_version: str
    result_limit: int = 100
    result_wait_ms: int = 200
    _request_queue: "queue.Queue[Optional[pb2.TaskStreamRequest]]" = field(default_factory=queue.Queue, init=False, repr=False)
    _result_queue: "queue.Queue[pb2.TaskResult]" = field(default_factory=queue.Queue, init=False, repr=False)
    _submit_waiters: Dict[str, "queue.Queue[pb2.TaskStreamSubmitAck]"] = field(default_factory=dict, init=False, repr=False)
    _cancel_waiters: Dict[str, "queue.Queue[pb2.TaskStreamCancelJobAck]"] = field(default_factory=dict, init=False, repr=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)
    _opened: threading.Event = field(default_factory=threading.Event, init=False, repr=False)
    _closed: threading.Event = field(default_factory=threading.Event, init=False, repr=False)
    _response_thread: Optional[threading.Thread] = field(default=None, init=False, repr=False)
    _fatal_error: Optional[BaseException] = field(default=None, init=False, repr=False)
    _close_sent: bool = field(default=False, init=False, repr=False)
    _node_credit: int = field(default=0, init=False, repr=False)

    def __post_init__(self) -> None:
        self.client_id = str(self.client_id or "").strip()
        self.code_version = str(self.code_version or "").strip()
        if not self.client_id:
            raise ValueError("client_id is required")
        if not self.code_version:
            raise ValueError("code_version is required")

        self._response_thread = threading.Thread(
            target=self._run_stream,
            name=f"task-stream-{self.client_id}",
            daemon=True,
        )
        self._response_thread.start()
        self._request_queue.put(
            pb2.TaskStreamRequest(
                open=pb2.TaskStreamOpen(
                    client_id=self.client_id,
                    code_version=self.code_version,
                    result_limit=max(1, int(self.result_limit)),
                    result_wait_ms=max(0, int(self.result_wait_ms)),
                )
            )
        )
        if not self._opened.wait(timeout=max(1.0, float(self._client.timeout_sec))):
            self.close()
            raise TimeoutError("task stream open timed out")
        self._raise_if_failed()

    def _request_iter(self) -> Iterator[pb2.TaskStreamRequest]:
        while True:
            item = self._request_queue.get()
            if item is None:
                return
            yield item

    def _set_fatal_error(self, exc: BaseException) -> None:
        with self._lock:
            if self._fatal_error is None:
                self._fatal_error = exc
        self._opened.set()
        self._closed.set()

    def _run_stream(self) -> None:
        try:
            responses = self._client.stub.TaskStream(self._request_iter())
            for resp in responses:
                kind = resp.WhichOneof("body")
                if kind == "open_ack":
                    self._node_credit = int(resp.open_ack.node_credit)
                    self._opened.set()
                    continue
                if kind == "submit_ack":
                    self._node_credit = int(resp.submit_ack.node_credit)
                    with self._lock:
                        waiter = self._submit_waiters.pop(resp.submit_ack.request_id, None)
                    if waiter is not None:
                        waiter.put(resp.submit_ack)
                    continue
                if kind == "cancel_job_ack":
                    with self._lock:
                        waiter = self._cancel_waiters.pop(resp.cancel_job_ack.request_id, None)
                    if waiter is not None:
                        waiter.put(resp.cancel_job_ack)
                    continue
                if kind == "result_batch":
                    self._node_credit = int(resp.result_batch.node_credit)
                    for item in resp.result_batch.results:
                        self._result_queue.put(item)
                    continue
                if kind == "credit_update":
                    self._node_credit = int(resp.credit_update.node_credit)
                    continue
                if kind == "error":
                    self._set_fatal_error(RuntimeError(resp.error.message or "task stream failed"))
                    break
                if kind == "closed":
                    self._node_credit = int(resp.closed.node_credit)
                    self._opened.set()
                    self._closed.set()
                    break
        except Exception as exc:
            self._set_fatal_error(exc)
        finally:
            self._opened.set()
            self._closed.set()

    def _raise_if_failed(self) -> None:
        if self._fatal_error is not None:
            raise RuntimeError(str(self._fatal_error))

    def _await_waiter(self, waiter: queue.Queue, *, timeout_sec: float, what: str):
        deadline = time.time() + max(0.1, float(timeout_sec))
        while time.time() < deadline:
            self._raise_if_failed()
            try:
                return waiter.get(timeout=min(0.1, max(0.01, deadline - time.time())))
            except queue.Empty:
                continue
        self._raise_if_failed()
        raise TimeoutError(f"{what} timed out")

    @property
    def node_credit(self) -> int:
        return int(self._node_credit)

    def submit_tasks(
        self,
        tasks: Sequence[pb2.TaskSubmitItem],
        *,
        execution_mode: int = pb2.EXECUTION_MODE_PERSISTENT,
        job_id: str = "",
        timeout_sec: float = 0.0,
    ) -> pb2.SubmitTasksResponse:
        self._raise_if_failed()
        request_id = uuid.uuid4().hex
        waiter: "queue.Queue[pb2.TaskStreamSubmitAck]" = queue.Queue(maxsize=1)
        with self._lock:
            self._submit_waiters[request_id] = waiter
        self._request_queue.put(
            pb2.TaskStreamRequest(
                submit=pb2.TaskStreamSubmit(
                    request_id=request_id,
                    job_id=str(job_id or "").strip(),
                    execution_mode=execution_mode,
                    tasks=list(tasks),
                )
            )
        )
        ack = self._await_waiter(
            waiter,
            timeout_sec=float(timeout_sec or self._client.timeout_sec or 10.0),
            what="task stream submit",
        )
        if ack.error and ack.error.message:
            raise RuntimeError(_err_msg(ack.error, "task stream submit failed"))
        return pb2.SubmitTasksResponse(
            ok=True,
            accepted=ack.accepted,
            rejected=ack.rejected,
            node_credit=ack.node_credit,
        )

    def cancel_job(
        self,
        *,
        job_id: str,
        reason: str = "",
        timeout_sec: float = 0.0,
    ) -> pb2.CancelJobResponse:
        self._raise_if_failed()
        request_id = uuid.uuid4().hex
        waiter: "queue.Queue[pb2.TaskStreamCancelJobAck]" = queue.Queue(maxsize=1)
        with self._lock:
            self._cancel_waiters[request_id] = waiter
        self._request_queue.put(
            pb2.TaskStreamRequest(
                cancel_job=pb2.TaskStreamCancelJob(
                    request_id=request_id,
                    job_id=str(job_id or "").strip(),
                    reason=reason,
                )
            )
        )
        ack = self._await_waiter(
            waiter,
            timeout_sec=float(timeout_sec or self._client.timeout_sec or 10.0),
            what="task stream cancel job",
        )
        if ack.error and ack.error.message:
            raise RuntimeError(_err_msg(ack.error, "task stream cancel job failed"))
        return pb2.CancelJobResponse(
            ok=True,
            queued_cancelled=ack.queued_cancelled,
            running_marked=ack.running_marked,
            already_done=ack.already_done,
            not_found=ack.not_found,
        )

    def pull_results(
        self,
        *,
        limit: int = 100,
        wait_ms: int = 0,
    ) -> pb2.PullResultsResponse:
        self._raise_if_failed()
        max_items = max(1, int(limit or 100))
        results: List[pb2.TaskResult] = []

        if wait_ms > 0 and self._result_queue.empty():
            try:
                first = self._result_queue.get(timeout=max(0.001, float(wait_ms) / 1000.0))
                results.append(first)
            except queue.Empty:
                return pb2.PullResultsResponse(ok=True, results=[], next_cursor="")

        while len(results) < max_items:
            try:
                results.append(self._result_queue.get_nowait())
            except queue.Empty:
                break
        return pb2.PullResultsResponse(ok=True, results=results, next_cursor="")

    def close(self, *, drain: bool = False) -> None:
        if self._close_sent:
            return
        self._close_sent = True
        self._request_queue.put(pb2.TaskStreamRequest(close=pb2.TaskStreamClose(drain=bool(drain))))
        self._request_queue.put(None)
        if self._response_thread is not None:
            self._response_thread.join(timeout=2.0)

    def __enter__(self) -> "TaskStreamSession":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()


@dataclass
class TaskBatchClient:
    """Multi-node task-mode helper bound to a selected set of NodeControl nodes."""

    _clients: Dict[str, NodeControlClient] = field(repr=False)
    _streams: Dict[str, TaskStreamSession] = field(repr=False)
    client_id: str
    job_id: str
    nodes: Dict[str, InfoCenterNode]
    code_version: str
    _cursors_by_node: Dict[str, str] = field(default_factory=dict, repr=False)
    _submit_seq: int = field(default=0, repr=False)
    _submitted_task_ids_by_job: Dict[str, List[str]] = field(default_factory=dict, repr=False)
    _seen_result_task_ids_by_job: Dict[str, Set[str]] = field(default_factory=dict, repr=False)
    _task_node_by_job: Dict[str, Dict[str, str]] = field(default_factory=dict, repr=False)
    _runtime_node_hint: Dict[str, str] = field(default_factory=dict, repr=False)
    _latest_credit_by_node: Dict[str, int] = field(default_factory=dict, repr=False)

    # 类级别的序列号计数器，确保同一毫秒内生成的ID也是唯一的
    _class_client_seq: int = 0
    _class_job_seq: int = 0

    @classmethod
    def from_infocenter(
        cls,
        *,
        infocenter_target: str,
        client_id: Optional[str] = None,
        job_id: Optional[str] = None,
        func: Optional[Callable] = None,
        module: Optional[Any] = None,
        code_version: str = "",
        artifact_path: str = "",
        blob: Optional[bytes] = None,
        filename: str = "",
        runtime: str = "py3",
        entry_module: str = "",
        entry_callable: str = "run",
        package_format: str = "",
        export_mode: str = "single",
        export_methods: Optional[Sequence[str]] = None,
        export_decorator: str = "pycloud_export",
        dependency_allowlist: Optional[Sequence[str]] = None,
        chunk_size: int = 256 * 1024,
        healthy_only: bool = True,
        tags: Optional[Sequence[str]] = None,
        node_ids: Optional[Sequence[str]] = None,
        node_count: int = 0,
        node_limit: int = 100,
        require_credit: bool = True,
        preferred_runtime_key: str = "",
        timeout_sec: float = 10.0,
    ) -> "TaskBatchClient":
        # 自动本地源码打包：处理模块对象和函数对象
        if module is not None:
            effective_blob, effective_filename = _prepare_code_blob(
                func=None,
                module=module,
                artifact_path="",
                blob=blob,
            )
            effective_filename = effective_filename or filename
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
            effective_filename = effective_filename or filename
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
            effective_filename = effective_filename or filename
            effective_package_format = package_format

        # 自动生成 client_id（如果未提供）
        effective_client_id = client_id
        if not effective_client_id:
            local_ip = _get_local_ip()
            timestamp_ms = int(time.time() * 1000)
            cls._class_client_seq += 1
            effective_client_id = f"client-{local_ip}-{timestamp_ms}-{cls._class_client_seq:04d}"

        # 自动生成 job_id（如果未提供）
        effective_job_id = job_id
        if not effective_job_id:
            local_ip = _get_local_ip()
            timestamp_ms = int(time.time() * 1000)
            cls._class_job_seq += 1
            effective_job_id = f"job-{local_ip}-{timestamp_ms}-{cls._class_job_seq:04d}"

        if not effective_client_id:
            raise ValueError("client_id is required")
        if not effective_job_id:
            raise ValueError("job_id is required")

        clients: Dict[str, NodeControlClient] = {}
        streams: Dict[str, TaskStreamSession] = {}
        try:
            effective_code_version = str(code_version or "").strip()
            if not effective_code_version:
                if effective_blob is not None:
                    effective_code_version = f"sha256:{hashlib.sha256(effective_blob).hexdigest()}"
                elif artifact_path:
                    upload_path = Path(artifact_path)
                    upload_file = upload_path
                    tmp_pkg: Optional[Path] = None
                    try:
                        if upload_path.is_dir():
                            tmp_pkg = _package_directory_to_targz(upload_path)
                            upload_file = tmp_pkg
                        effective_code_version = f"sha256:{_sha256_file(upload_file)}"
                    finally:
                        if tmp_pkg is not None:
                            tmp_pkg.unlink(missing_ok=True)
                else:
                    raise ValueError("code_version or artifact_path or blob or func must be provided")

            desired_runtime_key = str(preferred_runtime_key or effective_code_version).strip() or effective_code_version

            with InfoCenterClient(infocenter_target, timeout_sec=timeout_sec) as infocenter:
                selected_nodes = list(
                    infocenter.select_task_nodes(
                        healthy_only=healthy_only,
                        tags=tags,
                        node_ids=node_ids,
                        node_count=node_count,
                        limit=node_limit,
                        require_credit=require_credit,
                        preferred_runtime_key=desired_runtime_key,
                        runtime=runtime,
                    )
                )

            clients = {
                node.node_id: NodeControlClient(node.control_addr, timeout_sec=timeout_sec)
                for node in selected_nodes
            }

            if code_version:
                effective_code_version = str(code_version).strip()
            else:
                uploaded_versions: Dict[str, str] = {}
                if effective_blob is not None:
                    if not effective_filename:
                        raise ValueError("filename is required when blob is provided")
                    for node_id, client in clients.items():
                        upload = client.upload_code_from_bytes(
                            client_id=effective_client_id,
                            filename=effective_filename,
                            blob=effective_blob,
                            runtime=runtime,
                            entry_module=entry_module,
                            entry_callable=entry_callable,
                            package_format=effective_package_format,
                            export_mode=export_mode,
                            export_methods=export_methods,
                            export_decorator=export_decorator,
                            dependency_allowlist=dependency_allowlist,
                            chunk_size=chunk_size,
                        )
                        uploaded_versions[node_id] = upload.code_version
                elif artifact_path:
                    for node_id, client in clients.items():
                        upload = client.upload_code_from_file(
                            client_id=effective_client_id,
                            artifact_path=artifact_path,
                            filename=effective_filename,
                            runtime=runtime,
                            entry_module=entry_module,
                            entry_callable=entry_callable,
                            package_format=effective_package_format,
                            export_mode=export_mode,
                            export_methods=export_methods,
                            export_decorator=export_decorator,
                            dependency_allowlist=dependency_allowlist,
                            chunk_size=chunk_size,
                        )
                        uploaded_versions[node_id] = upload.code_version
                else:
                    raise ValueError("code_version or artifact_path or blob must be provided")

                unique_versions = {version for version in uploaded_versions.values() if str(version).strip()}
                if len(unique_versions) != 1:
                    raise RuntimeError(f"inconsistent code_version across nodes: {uploaded_versions}")
                effective_code_version = next(iter(unique_versions))

            for node_id, client in clients.items():
                streams[node_id] = client.open_task_stream(
                    client_id=effective_client_id,
                    code_version=effective_code_version,
                    result_limit=100,
                    result_wait_ms=200,
                )

            return cls(
                _clients=clients,
                _streams=streams,
                client_id=effective_client_id,
                job_id=effective_job_id,
                nodes={node.node_id: node for node in selected_nodes},
                code_version=effective_code_version,
                _cursors_by_node={node.node_id: "" for node in selected_nodes},
                _latest_credit_by_node={node.node_id: int(node.credit) for node in selected_nodes},
                _runtime_node_hint={desired_runtime_key: selected_nodes[0].node_id} if selected_nodes else {},
            )
        except Exception:
            for stream in streams.values():
                stream.close()
            for client in clients.values():
                client.close()
            raise

    def close(self) -> None:
        for stream in self._streams.values():
            stream.close()
        for client in self._clients.values():
            client.close()

    def __enter__(self) -> "TaskBatchClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def submit_tasks(
        self,
        tasks: Sequence[pb2.TaskSubmitItem],
        *,
        execution_mode: int = pb2.EXECUTION_MODE_PERSISTENT,
        job_id: str = "",
    ) -> pb2.SubmitTasksResponse:
        effective_job_id = str(job_id or self.job_id)
        if not tasks:
            return pb2.SubmitTasksResponse(ok=True, accepted=[], rejected=[], node_credit=0)

        task_by_id = {item.task_id: item for item in tasks}
        task_positions = {item.task_id: idx for idx, item in enumerate(tasks)}
        task_attempted_nodes: Dict[str, Set[str]] = {item.task_id: set() for item in tasks}
        accepted_by_task_id: Dict[str, pb2.TaskAccepted] = {}
        rejected_by_task_id: Dict[str, pb2.TaskRejected] = {}
        latest_credit_by_node: Dict[str, int] = dict(self._latest_credit_by_node or {})

        pending_task_ids = [item.task_id for item in tasks]
        while pending_task_ids:
            batches: Dict[str, List[pb2.TaskSubmitItem]] = {}
            for task_id in pending_task_ids:
                item = task_by_id[task_id]
                runtime_key = str(item.runtime_key or self.code_version).strip() or self.code_version
                node_order = self._node_order(latest_credit_by_node, runtime_key=runtime_key)
                target_node_id = next((node_id for node_id in node_order if node_id not in task_attempted_nodes[task_id]), "")
                if not target_node_id:
                    rejected_by_task_id[task_id] = pb2.TaskRejected(
                        task_id=task_id,
                        code=pb2.ERROR_CODE_NO_CREDIT,
                        message="no available task node accepted this task",
                    )
                    continue
                task_attempted_nodes[task_id].add(target_node_id)
                batches.setdefault(target_node_id, []).append(task_by_id[task_id])

            retriable_task_ids: List[str] = []
            for node_id, node_tasks in batches.items():
                stream = self._streams[node_id]
                node_task_ids = {item.task_id for item in node_tasks}
                try:
                    resp = stream.submit_tasks(
                        tasks=node_tasks,
                        execution_mode=execution_mode,
                        job_id=effective_job_id,
                    )
                    latest_credit_by_node[node_id] = int(stream.node_credit or resp.node_credit)
                    self._latest_credit_by_node[node_id] = latest_credit_by_node[node_id]
                except Exception as exc:
                    latest_credit_by_node[node_id] = 0
                    self._latest_credit_by_node[node_id] = 0
                    for task_id in node_task_ids:
                        if len(task_attempted_nodes[task_id]) < len(self._clients):
                            retriable_task_ids.append(task_id)
                        else:
                            rejected_by_task_id[task_id] = pb2.TaskRejected(
                                task_id=task_id,
                                code=pb2.ERROR_CODE_INTERNAL_ERROR,
                                message=f"submit to node {node_id} failed: {exc}",
                            )
                    continue

                accepted_ids = {item.task_id for item in resp.accepted}
                submitted = self._submitted_task_ids_by_job.setdefault(effective_job_id, [])
                task_node_map = self._task_node_by_job.setdefault(effective_job_id, {})
                for item in resp.accepted:
                    accepted_by_task_id[item.task_id] = item
                    task_node_map[item.task_id] = node_id
                    runtime_key = str(task_by_id[item.task_id].runtime_key or self.code_version).strip() or self.code_version
                    self._runtime_node_hint[runtime_key] = node_id
                    if item.task_id not in submitted:
                        submitted.append(item.task_id)

                for item in resp.rejected:
                    if (
                        item.code in (pb2.ERROR_CODE_NO_CREDIT, pb2.ERROR_CODE_QUEUE_FULL)
                        and len(task_attempted_nodes[item.task_id]) < len(self._clients)
                    ):
                        retriable_task_ids.append(item.task_id)
                    else:
                        rejected_by_task_id[item.task_id] = item

                unresolved_ids = node_task_ids - accepted_ids - {item.task_id for item in resp.rejected}
                for task_id in unresolved_ids:
                    if len(task_attempted_nodes[task_id]) < len(self._clients):
                        retriable_task_ids.append(task_id)
                    else:
                        rejected_by_task_id[task_id] = pb2.TaskRejected(
                            task_id=task_id,
                            code=pb2.ERROR_CODE_INTERNAL_ERROR,
                            message=f"node {node_id} returned no final status for task",
                        )

            pending_task_ids = []
            seen_retry: Set[str] = set()
            for task_id in retriable_task_ids:
                if task_id in accepted_by_task_id or task_id in rejected_by_task_id or task_id in seen_retry:
                    continue
                seen_retry.add(task_id)
                pending_task_ids.append(task_id)

        accepted = sorted(accepted_by_task_id.values(), key=lambda item: task_positions.get(item.task_id, 0))
        rejected = sorted(rejected_by_task_id.values(), key=lambda item: task_positions.get(item.task_id, 0))
        return pb2.SubmitTasksResponse(
            ok=True,
            accepted=accepted,
            rejected=rejected,
            node_credit=sum(max(0, int(value)) for value in latest_credit_by_node.values()),
        )

    def submit_payloads(
        self,
        payloads: Sequence[Dict[str, object]],
        *,
        execution_mode: int = pb2.EXECUTION_MODE_PERSISTENT,
        job_id: str = "",
        task_id_prefix: str = "",
        timeout_hint_sec: int = 0,
        priority: int = 1,
        runtime_key: str = "",
    ) -> pb2.SubmitTasksResponse:
        effective_job_id = str(job_id or self.job_id)
        normalized_runtime_key = str(runtime_key or self.code_version).strip() or self.code_version
        prefix = str(task_id_prefix or f"{effective_job_id}-task").strip()
        items: List[pb2.TaskSubmitItem] = []
        for payload in payloads:
            self._submit_seq += 1
            items.append(
                pb2.TaskSubmitItem(
                    task_id=f"{prefix}-{self._submit_seq:04d}",
                    payload=dict_to_struct(_serialize_arrow_compatible(payload or {})),
                    timeout_hint_sec=max(0, int(timeout_hint_sec)),
                    priority=max(1, int(priority)),
                    runtime_key=normalized_runtime_key,
                )
            )
        return self.submit_tasks(items, execution_mode=execution_mode, job_id=effective_job_id)

    def pull_results(
        self,
        *,
        limit: int = 100,
        wait_ms: int = 0,
        cursor: str = "",
    ) -> pb2.PullResultsResponse:
        del cursor
        max_items = max(1, int(limit or 100))
        results: List[pb2.TaskResult] = []
        deadline = time.time() + max(0.0, float(wait_ms) / 1000.0)

        while True:
            for node_id, stream in self._streams.items():
                if len(results) >= max_items:
                    break
                resp = stream.pull_results(limit=max_items - len(results), wait_ms=0)
                results.extend(resp.results)
                self._latest_credit_by_node[node_id] = int(stream.node_credit)
            if results or wait_ms <= 0 or time.time() >= deadline:
                break
            time.sleep(0.02)

        results.sort(key=lambda item: (str(item.job_id or ""), item.task_id))
        return pb2.PullResultsResponse(ok=True, results=results, next_cursor="")

    def pull_new_results(
        self,
        *,
        limit: int = 100,
        wait_ms: int = 0,
        job_id: str = "",
    ) -> Sequence[pb2.TaskResult]:
        resp = self.pull_results(limit=limit, wait_ms=wait_ms, cursor="")
        out = list(resp.results)
        for item in out:
            seen = self._seen_result_task_ids_by_job.setdefault(str(item.job_id or ""), set())
            seen.add(item.task_id)
        effective_job_id = str(job_id or "")
        if not effective_job_id:
            return out
        return [item for item in out if str(item.job_id or "") == effective_job_id]

    def wait_for_results(
        self,
        *,
        expected_count: int = 0,
        timeout_sec: float = 30.0,
        wait_ms: int = 500,
        limit: int = 100,
        job_id: str = "",
    ) -> Sequence[pb2.TaskResult]:
        effective_job_id = str(job_id or self.job_id)
        remaining = int(expected_count or 0)
        if remaining <= 0:
            submitted = self._submitted_task_ids_by_job.get(effective_job_id, [])
            seen = self._seen_result_task_ids_by_job.get(effective_job_id, set())
            remaining = max(0, len(submitted) - len(seen))

        deadline = time.time() + max(0.1, float(timeout_sec))
        results: List[pb2.TaskResult] = []
        while time.time() < deadline and len(results) < remaining:
            batch = list(self.pull_new_results(limit=limit, wait_ms=wait_ms, job_id=effective_job_id))
            if batch:
                results.extend(batch)
                continue
            if remaining <= 0:
                break
        return results

    def submitted_task_ids(self, *, job_id: str = "") -> Sequence[str]:
        effective_job_id = str(job_id or self.job_id)
        return list(self._submitted_task_ids_by_job.get(effective_job_id, ()))

    def cancel_tasks(
        self,
        task_ids: Sequence[str],
        *,
        reason: str = "",
    ) -> pb2.CancelTasksResponse:
        task_ids = [str(task_id) for task_id in task_ids if str(task_id).strip()]
        status_by_task_id: Dict[str, str] = {task_id: "not_found" for task_id in task_ids}
        grouped: Dict[str, List[str]] = {}
        unknown: List[str] = []
        for task_id in task_ids:
            node_id = self._lookup_task_node(task_id)
            if node_id:
                grouped.setdefault(node_id, []).append(task_id)
            else:
                unknown.append(task_id)

        for node_id, ids in grouped.items():
            resp = self._clients[node_id].cancel_tasks(
                client_id=self.client_id,
                task_ids=ids,
                reason=reason,
            )
            for task_id in resp.cancelled:
                status_by_task_id[task_id] = "cancelled"
            for task_id in resp.already_done:
                if status_by_task_id.get(task_id) != "cancelled":
                    status_by_task_id[task_id] = "already_done"

        if unknown:
            for node_id, client in self._clients.items():
                resp = client.cancel_tasks(
                    client_id=self.client_id,
                    task_ids=unknown,
                    reason=reason,
                )
                for task_id in resp.cancelled:
                    status_by_task_id[task_id] = "cancelled"
                for task_id in resp.already_done:
                    if status_by_task_id.get(task_id) != "cancelled":
                        status_by_task_id[task_id] = "already_done"

        cancelled = [task_id for task_id in task_ids if status_by_task_id.get(task_id) == "cancelled"]
        already_done = [task_id for task_id in task_ids if status_by_task_id.get(task_id) == "already_done"]
        not_found = [task_id for task_id in task_ids if status_by_task_id.get(task_id) == "not_found"]
        return pb2.CancelTasksResponse(
            ok=True,
            cancelled=cancelled,
            already_done=already_done,
            not_found=not_found,
        )

    def cancel_job(self, *, reason: str = "", job_id: str = "") -> pb2.CancelJobResponse:
        effective_job_id = str(job_id or self.job_id)
        queued_cancelled = 0
        running_marked = 0
        already_done = 0
        matched = 0
        for stream in self._streams.values():
            resp = stream.cancel_job(
                job_id=effective_job_id,
                reason=reason,
            )
            queued_cancelled += int(resp.queued_cancelled)
            running_marked += int(resp.running_marked)
            already_done += int(resp.already_done)
            if int(resp.not_found or 0) == 0:
                matched += 1
        return pb2.CancelJobResponse(
            ok=True,
            queued_cancelled=queued_cancelled,
            running_marked=running_marked,
            already_done=already_done,
            not_found=0 if matched or queued_cancelled or running_marked or already_done else 1,
        )

    def get_metrics(self) -> Dict[str, pb2.GetMetricsResponse]:
        return {
            node_id: client.get_metrics()
            for node_id, client in self._clients.items()
        }

    @property
    def node_ids(self) -> Sequence[str]:
        return list(self.nodes.keys())

    def _lookup_task_node(self, task_id: str) -> str:
        for task_map in self._task_node_by_job.values():
            node_id = task_map.get(task_id, "")
            if node_id:
                return node_id
        return ""

    def _node_order(self, latest_credit_by_node: Dict[str, int], *, runtime_key: str = "") -> List[str]:
        ordered_nodes = list(self.nodes.values())
        sticky_node_id = str(self._runtime_node_hint.get(str(runtime_key or "").strip(), "")).strip()
        ordered_nodes.sort(
            key=lambda node: (
                0 if sticky_node_id and node.node_id == sticky_node_id else 1,
                -int(latest_credit_by_node.get(node.node_id, node.credit)),
                int(node.queued),
                int(node.inflight),
                node.node_id,
            )
        )
        return [node.node_id for node in ordered_nodes]


@dataclass
class ServiceGroup:
    """A deployed service group spread across multiple NodeControl nodes."""

    owner_client_id: str
    service_name: str
    sessions: Dict[str, ServiceSessionClient]
    nodes: Dict[str, InfoCenterNode]
    failures: Dict[str, str] = field(default_factory=dict)
    breaker_enabled: bool = True
    breaker_failure_threshold: int = 3
    breaker_cooldown_sec: float = 15.0
    breaker_max_cooldown_sec: float = 120.0
    _clients: Dict[str, NodeControlClient] = field(default_factory=dict, repr=False)
    _session_cache_file: Optional[Path] = field(default=None, repr=False)
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
        artifact_path: str = "",
        artifact_paths: Optional[Sequence[str]] = None,
        blob: Optional[bytes] = None,
        filename: str = "",
        runtime: str = "py3",
        entry_module: str = "",
        entry_callable: str = "run",
        package_format: str = "",
        export_mode: str = "decorator",
        export_methods: Optional[Sequence[str]] = None,
        export_decorator: str = "pycloud_export",
        dependency_allowlist: Optional[Sequence[str]] = None,
        worker_count: int = 10,
        heartbeat_timeout_sec: int = 30,
        idle_ttl_sec: int = 0,
        expose_http: bool = True,
        chunk_size: int = 256 * 1024,
        healthy_only: bool = True,
        tags: Optional[Sequence[str]] = None,
        node_ids: Optional[Sequence[str]] = None,
        node_count: int = 0,
        node_limit: int = 100,
        allow_partial: bool = True,
        min_success_nodes: int = 1,
        timeout_sec: float = 10.0,
        ensure_unique_service_name: bool = True,
        reuse_existing_same_code: bool = True,
        replace_existing_if_code_changed: bool = False,
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
            artifact_path: 单个文件路径
            artifact_paths: 文件/文件夹路径列表，会自动打包成 zip
            blob: 直接提供代码内容
            filename: 文件名
            runtime: 运行时版本
            entry_module: 入口模块名
            entry_callable: 入口函数名
            package_format: 包格式 ("py", "zip", "tar.gz")
            export_mode: 导出模式 ("decorator", "explicit", "all", "single")
            export_methods: 显式导出的方法列表
            export_decorator: 装饰器名称
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
            replace_existing_if_code_changed: 同 owner + 同服务名但代码变化时是否替换
            session_cache_dir: 本地 service session token 缓存目录
            breaker_enabled: 是否启用熔断器
            breaker_failure_threshold: 熔断失败阈值
            breaker_cooldown_sec: 熔断冷却时间
            breaker_max_cooldown_sec: 熔断最大冷却时间

        Returns:
            ServiceGroup: 部署的服务组
        """
        # 自动本地源码打包：处理模块对象和函数对象
        if module is not None:
            effective_blob, effective_filename = _prepare_code_blob(
                func=None,
                module=module,
                artifact_path="",
                blob=blob,
            )
            effective_filename = effective_filename or filename
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
            effective_filename = effective_filename or filename
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
            effective_filename = effective_filename or filename
            effective_package_format = package_format

        # 生成默认的 owner_client_id 和 service_name
        local_ip = _get_local_ip()

        # 如果 owner_client_id 为空，使用本机 IP
        effective_owner_client_id = owner_client_id
        if not effective_owner_client_id:
            effective_owner_client_id = f"client-{local_ip}"

        # 先确定 entry_module（用于生成 service_name）
        effective_entry_module = entry_module
        if not effective_entry_module:
            if effective_filename:
                # 优先使用 filename
                if effective_filename.endswith(".py"):
                    effective_entry_module = Path(effective_filename).stem
            else:
                # 尝试从 artifact_path 推断
                if artifact_path:
                    path = Path(artifact_path)
                    if path.suffix == ".py":
                        effective_entry_module = path.stem
                # 尝试从 artifact_paths 推断
                elif artifact_paths and len(artifact_paths) > 0:
                    first_path = Path(artifact_paths[0])
                    if first_path.suffix == ".py":
                        effective_entry_module = first_path.stem

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

        # 优先级: func > blob > artifact_path > artifact_paths
        # 如果 func 参数已处理，effective_blob 已经设置，跳过后续处理
        if effective_blob is None and artifact_paths:
            # artifact_paths 列表：打包成 zip
            import zipfile
            import tempfile

            paths = [str(p) for p in artifact_paths if p]
            if not paths:
                raise ValueError("artifact_paths list is empty")

            # 创建临时 zip 文件
            with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp_zip:
                tmp_zip_path = tmp_zip.name

            with zipfile.ZipFile(tmp_zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
                for path_str in paths:
                    p = Path(path_str)
                    if not p.exists():
                        raise FileNotFoundError(f"Path not found: {p}")
                    if p.is_file():
                        # 单个文件：直接添加，保持原名
                        zf.write(p, p.name)
                    elif p.is_dir():
                        # 收集所有需要写入 zip 的目录（用于自动补全 __init__.py）
                        dirs_to_check = {p}  # 顶级目录本身
                        for child in p.rglob('*'):
                            if child.is_dir():
                                dirs_to_check.add(child)

                        # 为缺少 __init__.py 的目录写入空文件
                        for d in sorted(dirs_to_check, key=lambda x: str(x)):
                            if not (d / "__init__.py").exists():
                                init_arcname = p.name / d.relative_to(p) / "__init__.py"
                                zf.writestr(str(init_arcname), "")

                        # 文件夹：将文件夹本身作为 zip 内的一层目录，写入所有文件
                        for file_path in p.rglob('*'):
                            if file_path.is_file():
                                arcname = p.name / file_path.relative_to(p)
                                zf.write(file_path, str(arcname))
            if not effective_filename:
                effective_filename = Path(tmp_zip_path).name
            effective_blob = Path(tmp_zip_path).read_bytes()
            effective_package_format = "zip"

            # 清理临时文件
            try:
                os.unlink(tmp_zip_path)
            except Exception:
                pass

        if effective_blob is None:
            if not artifact_path:
                raise ValueError("artifact_path or artifact_paths or blob must be provided")
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

        effective_code_version = _artifact_code_version(effective_blob)
        session_cache_file = _service_session_cache_file(
            owner_client_id=effective_owner_client_id,
            service_name=effective_service_name,
            cache_dir=session_cache_dir,
        )

        requested_node_ids = [str(node_id).strip() for node_id in (node_ids or []) if str(node_id).strip()]
        desired_node_count = max(0, int(node_count or 0))
        required_success_nodes = max(1, int(min_success_nodes))
        discovery_limit = max(
            1,
            int(node_limit),
            len(requested_node_ids),
            desired_node_count or required_success_nodes,
        )

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

        if not discovered_nodes:
            raise RuntimeError("no available nodes from InfoCenter")

        normalized_runtime = normalize_python_runtime_spec(runtime)
        discovered_node_map = {node.node_id: node for node in discovered_nodes}
        if requested_node_ids:
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
                    raise RuntimeError(
                        f"no schedulable nodes from InfoCenter for runtime {normalized_runtime}"
                    )
                raise RuntimeError("no schedulable nodes from InfoCenter")
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
                    return cls._reuse_existing_group(
                        owner_client_id=effective_owner_client_id,
                        service_name=effective_service_name,
                        artifact_code_version=effective_code_version,
                        cache_payload=cached_session,
                        active_routes=existing_infos,
                        discovered_node_map=discovered_node_map,
                        timeout_sec=timeout_sec,
                        breaker_enabled=breaker_enabled,
                        breaker_failure_threshold=breaker_failure_threshold,
                        breaker_cooldown_sec=breaker_cooldown_sec,
                        breaker_max_cooldown_sec=breaker_max_cooldown_sec,
                        session_cache_file=session_cache_file,
                    )

                if not replace_existing_if_code_changed:
                    raise RuntimeError(
                        f"service_name already exists with different code_version: {effective_service_name}; "
                        f"existing={existing_code_version}; incoming={effective_code_version}; "
                        "set replace_existing_if_code_changed=True to replace"
                    )
                if cached_session is None:
                    raise RuntimeError(
                        f"service_name already exists with different code_version but no local token cache was found for replacement: "
                        f"{effective_service_name}"
                    )

                cls._end_existing_group(
                    owner_client_id=effective_owner_client_id,
                    cache_payload=cached_session,
                    active_routes=existing_infos,
                    timeout_sec=timeout_sec,
                    reason=f"replace service due to code change: {effective_code_version}",
                )
                try:
                    session_cache_file.unlink()
                except FileNotFoundError:
                    pass

        sessions: Dict[str, ServiceSessionClient] = {}
        clients: Dict[str, NodeControlClient] = {}
        nodes: Dict[str, InfoCenterNode] = {}
        failures: Dict[str, str] = {}

        for node in selected_nodes:
            client = NodeControlClient(node.control_addr, timeout_sec=timeout_sec)
            try:
                session = client.create_service_from_bytes(
                    owner_client_id=effective_owner_client_id,
                    service_name=effective_service_name,
                    filename=effective_filename,
                    blob=effective_blob,
                    runtime=runtime,
                    entry_module=effective_entry_module,
                    entry_callable=entry_callable,
                    package_format=effective_package_format,
                    export_mode=export_mode,
                    export_methods=export_methods,
                    export_decorator=export_decorator,
                    dependency_allowlist=dependency_allowlist,
                    worker_count=worker_count,
                    heartbeat_timeout_sec=heartbeat_timeout_sec,
                    idle_ttl_sec=idle_ttl_sec,
                    expose_http=expose_http,
                    chunk_size=chunk_size,
                )
            except Exception as exc:
                failures[node.node_id] = repr(exc)
                client.close()
                if not allow_partial:
                    cls._cleanup_created_services(sessions=sessions, clients=clients, reason="rollback deploy")
                    raise RuntimeError(f"deploy failed on node={node.node_id}: {exc}") from exc
                continue

            sessions[node.node_id] = session
            clients[node.node_id] = client
            nodes[node.node_id] = node

        if len(sessions) < required_success_nodes:
            cls._cleanup_created_services(sessions=sessions, clients=clients, reason="insufficient success nodes")
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
            _artifact_code_version=effective_code_version,
        )
        group._persist_session_cache()
        group._start_keepalive()
        return group

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
                failures[route.node_id] = repr(exc)
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
    ) -> "ServiceGroup":
        cache_nodes = cache_payload.get("nodes")
        if not isinstance(cache_nodes, dict):
            raise RuntimeError("invalid local service session cache: nodes missing")

        sessions: Dict[str, ServiceSessionClient] = {}
        clients: Dict[str, NodeControlClient] = {}
        nodes: Dict[str, InfoCenterNode] = {}

        try:
            for route, info in active_routes:
                node = discovered_node_map.get(route.node_id)
                if node is None:
                    raise RuntimeError(
                        f"existing service route is outside current discovery scope: node_id={route.node_id}"
                    )

                cached_node = cache_nodes.get(route.node_id)
                if not isinstance(cached_node, dict):
                    raise RuntimeError(
                        f"local service session cache missing node entry for reuse: node_id={route.node_id}"
                    )

                cached_service_id = str(cached_node.get("service_id", "")).strip()
                cached_token = str(cached_node.get("service_token", "")).strip()
                if cached_service_id != route.service_id:
                    raise RuntimeError(
                        f"local service session cache is stale for node={route.node_id}: "
                        f"cached_service_id={cached_service_id} route_service_id={route.service_id}"
                    )
                if not cached_token:
                    raise RuntimeError(f"local service session cache missing token for node={route.node_id}")

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

                sessions[route.node_id] = ServiceSessionClient(
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
                clients[route.node_id] = client
                nodes[route.node_id] = node
        except Exception:
            for client in clients.values():
                try:
                    client.close()
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
            cached_node = cache_nodes.get(route.node_id)
            if not isinstance(cached_node, dict):
                failures[route.node_id] = "missing cached node entry"
                continue
            cached_service_id = str(cached_node.get("service_id", "")).strip()
            cached_token = str(cached_node.get("service_token", "")).strip()
            if cached_service_id != route.service_id or not cached_token:
                failures[route.node_id] = "stale or missing cached token"
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
                failures[route.node_id] = repr(exc)
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
        for node_id, session in sorted(self.sessions.items()):
            node = self.nodes.get(node_id)
            control_addr = ""
            if node is not None:
                control_addr = node.control_addr
            elif node_id in self._clients:
                control_addr = self._clients[node_id].target
            nodes_payload[node_id] = {
                "control_addr": control_addr,
                "service_id": session.service_id,
                "service_token": session.service_token,
                "http_base_url": session.http_base_url,
                "heartbeat_timeout_sec": int(session.heartbeat_timeout_sec),
                "worker_count": int(session.worker_count),
            }
        payload["nodes"] = nodes_payload
        _write_private_json(self._session_cache_file, payload)

    def _clear_session_cache(self) -> None:
        if self._session_cache_file is None:
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

    def __enter__(self) -> "ServiceGroup":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close(end_services=False)

    def node_ids(self) -> Sequence[str]:
        return list(self.sessions.keys())

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
        for node_id, session in self.sessions.items():
            out[node_id] = session.get_status()
        return out

    def call_on_node(
        self,
        node_id: str,
        method: str,
        payload: Dict[str, object],
        *,
        timeout_sec: float = 60.0,
    ) -> Dict[str, object]:
        session = self.sessions.get(node_id)
        if session is None:
            raise KeyError(f"unknown node_id: {node_id}")
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
            idx = self._route_index % len(candidates)
            self._route_index += 1
            return candidates[idx]

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
                    lambda: self.sessions[node_id].call(method, serialized_payload, timeout_sec=timeout_sec),
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
                        lambda: self.sessions[node_id].call(method, payload, timeout_sec=timeout_sec),
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

        _, resp = await self._group.acall_balanced(
            self._method,
            serialized_payload,
            timeout_sec=self._timeout_sec,
            strategy=self._strategy,
            refresh_status=self._refresh_status,
        )
        return resp.get("data", resp)

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

        _, resp = self._group.call_balanced(
            self._method,
            serialized_payload,
            timeout_sec=self._timeout_sec,
            strategy=self._strategy,
            refresh_status=self._refresh_status,
        )
        return resp.get("data", resp)


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
    ) -> List[Tuple[Optional[str], Optional[Dict[str, object]], Optional[Exception]]]:
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
        return await self._group.acall_all(
            self._method,
            kwargs,
            timeout_sec=self._timeout_sec,
            max_concurrency=self._max_concurrency,
        )

    def __await__(self):
        return self().__await__()


class ServiceModuleGroup(ServiceGroup):
    """模块化的服务组，像使用 Python 模块一样调用远程服务。

    支持多种调用方式：
    - await group.square(x=7)        # 异步调用
    - group.square.sync(x=7)         # 同步调用
    - await group.square.broadcast() # 广播到所有节点
    - group.list_methods()           # 列出所有可用方法

    Example:
        >>> group = ServiceModuleGroup.deploy_from_infocenter(...)
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
        _, resp = await self.acall_balanced(method, kwargs)
        return resp.get("data", resp)

    def call_sync(self, method: str, **kwargs) -> Dict[str, object]:
        """通用同步调用接口。

        Args:
            method: 方法名
            **kwargs: 方法参数

        Returns:
            Dict[str, object]: 服务的返回值
        """
        _, resp = self.call_balanced(method, kwargs)
        return resp.get("data", resp)

    async def call_all(
        self,
        method: str,
        **kwargs,
    ) -> List[Tuple[Optional[str], Optional[Dict[str, object]], Optional[Exception]]]:
        """异步调用所有节点。

        Args:
            method: 方法名
            **kwargs: 方法参数

        Returns:
            List[Tuple[节点ID, 结果, 异常]]: 所有节点的结果
        """
        return await self.acall_all(method, kwargs)

    def __repr__(self) -> str:
        node_ids = list(self.sessions.keys()) if self.sessions else []
        methods = self.methods if self._discovered_methods is not None else ["<not discovered>"]
        return (
            f"<ServiceModuleGroup "
            f"service={self.service_name!r} "
            f"nodes={len(node_ids)} "
            f"methods={methods[:3]}{'...' if len(methods) > 3 else ''}>"
        )

class GatewayModuleClient(GatewayServiceClient):
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
    ) -> None:
        super().__init__(target, timeout_sec=timeout_sec, service_token=service_token)
        self.service_name = str(service_name or "").strip()
        if not self.service_name:
            raise ValueError("service_name is required")
        self._discovered_methods: Optional[List[str]] = None

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
            self._discovered_methods = [str(item.get("method", "")).strip() for item in methods if str(item.get("method", "")).strip()]
        except Exception:
            self._discovered_methods = []

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
        return self.get_status(service_name=self.service_name)

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
        _, resp = await self.acall_balanced(method, kwargs, timeout_sec=self.timeout_sec)
        return resp.get("data", resp)

    def call_sync(self, method: str, **kwargs) -> Dict[str, object]:
        _, resp = self.call_balanced(method, kwargs, timeout_sec=self.timeout_sec)
        return resp.get("data", resp)

    async def acall_all(
        self,
        method: str,
        payload: Dict[str, object],
        *,
        timeout_sec: float = 60.0,
        max_concurrency: int = 100,
    ) -> List[Tuple[Optional[str], Optional[Dict[str, object]], Optional[Exception]]]:
        del method, payload, timeout_sec, max_concurrency
        raise NotImplementedError("GatewayModuleClient does not support broadcast; use Gateway for single-route calls")

    def __repr__(self) -> str:
        methods = self.methods if self._discovered_methods is not None else ["<not discovered>"]
        return (
            f"<GatewayModuleClient "
            f"service={self.service_name!r} "
            f"methods={methods[:3]}{'...' if len(methods) > 3 else ''}>"
        )


class DiscoveryModuleClient(DiscoveryServiceClient):
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
            self._discovered_methods = [str(item.get("method", "")).strip() for item in methods if str(item.get("method", "")).strip()]
        except Exception:
            self._discovered_methods = []

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
        return self.get_status(service_name=self.service_name)

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
            return route.node_id, resp
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
                return retry_route.node_id, resp
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
        _, resp = await self.acall_balanced(method, kwargs, timeout_sec=self.timeout_sec)
        return resp.get("data", resp)

    def call_sync(self, method: str, **kwargs) -> Dict[str, object]:
        _, resp = self.call_balanced(method, kwargs, timeout_sec=self.timeout_sec)
        return resp.get("data", resp)

    async def acall_all(
        self,
        method: str,
        payload: Dict[str, object],
        *,
        timeout_sec: float = 60.0,
        max_concurrency: int = 100,
    ) -> List[Tuple[Optional[str], Optional[Dict[str, object]], Optional[Exception]]]:
        del method, payload, timeout_sec, max_concurrency
        raise NotImplementedError("DiscoveryModuleClient does not support broadcast; use direct discovery for single-route calls")

    def __repr__(self) -> str:
        methods = self.methods if self._discovered_methods is not None else ["<not discovered>"]
        return (
            f"<DiscoveryModuleClient "
            f"service={self.service_name!r} "
            f"methods={methods[:3]}{'...' if len(methods) > 3 else ''}>"
        )


class _TaskCallProxy:
    """任务方法调用代理。

    提供类似函数调用的方式提交任务并获取结果。
    """

    def __init__(
        self,
        payload: Dict[str, object],
        batch: TaskBatchClient,
        *,
        timeout_hint_sec: int = 0,
        priority: int = 1,
        runtime_key: str = "",
    ) -> None:
        self._payload = payload
        self._batch = batch
        self._timeout_hint_sec = timeout_hint_sec
        self._priority = priority
        self._runtime_key = runtime_key

    def __repr__(self) -> str:
        return f"<_TaskCallProxy payload={self._payload!r}>"

    @property
    def payload(self) -> Dict[str, object]:
        """返回任务的 payload。"""
        return self._payload

    def submit(
        self,
        *args,
        timeout_hint_sec: Optional[float] = None,
        priority: Optional[int] = None,
        runtime_key: Optional[str] = None,
        **kwargs,
    ) -> pb2.SubmitTasksResponse:
        """提交任务，不等待结果。

        Args:
            *args: 任务的位置参数
            timeout_hint_sec: 超时提示（框架控制参数，不传给任务函数）
            priority: 优先级（框架控制参数，不传给任务函数）
            runtime_key: 运行时键（框架控制参数，不传给任务函数）
            **kwargs: 任务的命名参数

        Returns:
            pb2.SubmitTasksResponse: 提交响应

        Example:
            >>> # 位置参数
            >>> resp = task.submit(7)
            >>> # 命名参数
            >>> resp = task.submit(value=7)
            >>> # 混合参数
            >>> resp = task.submit(7, sleep_ms=100)
            >>> # 带控制参数
            >>> resp = task.submit(7, timeout_hint_sec=60, priority=1)
        """
        # 使用传入的控制参数，或回退到默认值
        timeout_hint = timeout_hint_sec if timeout_hint_sec is not None else self._timeout_hint_sec
        prio = priority if priority is not None else self._priority
        rt_key = runtime_key if runtime_key is not None else self._runtime_key

        # 构造 payload（只包含业务参数）
        payload = dict(self._payload)

        # 只有 kwargs 时保持旧格式；存在 args 时使用 args/kwargs 格式。
        if args:
            payload["args"] = list(args)
            if kwargs:
                payload["kwargs"] = kwargs
        elif kwargs:
            payload.update(kwargs)

        serialized_payload = _serialize_arrow_compatible(payload)

        # 打印任务提交信息
        print(f"[gRPC SubmitTasks] payload={json.dumps(serialized_payload, ensure_ascii=False)}")

        return self._batch.submit_payloads(
            [serialized_payload],
            timeout_hint_sec=timeout_hint,
            priority=prio,
            runtime_key=rt_key,
        )

    def submit_and_wait(
        self,
        *,
        timeout_sec: float = 30.0,
        wait_ms: int = 500,
        _payload=None,
        **kwargs,
    ) -> Sequence[pb2.TaskResult]:
        """提交任务并等待结果。

        Args:
            timeout_sec: 总超时时间
            wait_ms: 每次轮询等待时间
            _payload: 内部使用，自定义 payload
            **kwargs: 额外的提交参数

        Returns:
            Sequence[pb2.TaskResult]: 结果列表
        """
        if _payload is not None:
            # 直接提交指定的 payload
            resp = self._batch.submit_payloads(
                [_payload],
                timeout_hint_sec=kwargs.pop('timeout_hint_sec', self._timeout_hint_sec),
                priority=kwargs.pop('priority', self._priority),
                runtime_key=kwargs.pop('runtime_key', self._runtime_key),
            )
        else:
            resp = self.submit(**kwargs)

        if not resp.accepted:
            raise RuntimeError(f"task rejected: {resp.rejected}")

        expected_count = len(resp.accepted)
        results = self._batch.wait_for_results(
            expected_count=expected_count,
            timeout_sec=timeout_sec,
            wait_ms=wait_ms,
        )
        return results

    def __call__(self, *args, **kwargs) -> Sequence[pb2.TaskResult]:
        """直接调用：提交任务并等待结果（简写）。

        Args:
            *args: 位置参数
            **kwargs: 任务参数��会合并到 payload 中）

        Returns:
            Sequence[pb2.TaskResult]: 结果列表

        Example:
            >>> # 命名参数
            >>> results = task.run(value=7)
            >>> # 位置参数
            >>> results = task.run(7)
        """
        # 只有 kwargs 时保持旧格式；存在 args 时使用 args/kwargs 格式。
        if args or kwargs:
            payload = dict(self._payload)
            if args:
                payload["args"] = list(args)
                if kwargs:
                    payload["kwargs"] = kwargs
            else:
                payload.update(kwargs)
            serialized_payload = _serialize_arrow_compatible(payload)
            return self.submit_and_wait(
                timeout_hint_sec=self._timeout_hint_sec,
                priority=self._priority,
                runtime_key=self._runtime_key,
                _payload=serialized_payload,  # 使用内部参数名避免冲突
            )

        return self.submit_and_wait()


class TaskModuleClient:
    """任务模式的模块化客户端。

    提供类似 Python 模块的调用方式来提交任务。

    特点：
    - 像 function 一样提交任务
    - 自动处理 payload 序列化
    - 支持提交后等待或异步获取结果
    - 简化 TaskBatchClient 的使用

    Example:
        >>> from pycloud_parallel.controlplane.client import TaskModuleClient
        >>>
        >>> # 创建任务客户端
        >>> task = TaskModuleClient.from_infocenter(
        ...     infocenter_target="127.0.0.1:50051",
        ...     blob=blob,
        ...     filename="task.py",
        ... )
        >>>
        >>> # 提交任务并等待结果
        >>> results = task.run(x=1, y=2)
        >>> for result in results:
        ...     print(result.status, result.result)
        >>>
        >>> # 或者只提交，稍后获取结果
        >>> resp = task.run.submit(x=1, y=2)
        >>> results = task.wait_for_results(expected_count=1)
    """

    _batch: TaskBatchClient

    def __init__(self, batch: TaskBatchClient) -> None:
        """初始化 TaskModuleClient。

        Args:
            batch: 底层的 TaskBatchClient 实例
        """
        self._batch = batch

    @classmethod
    def from_infocenter(
        cls,
        *,
        infocenter_target: str,
        client_id: Optional[str] = None,
        job_id: Optional[str] = None,
        code_version: str = "",
        artifact_path: str = "",
        blob: Optional[bytes] = None,
        filename: str = "",
        runtime: str = "py3",
        entry_module: str = "",
        entry_callable: str = "run",
        package_format: str = "",
        export_mode: str = "single",
        export_methods: Optional[Sequence[str]] = None,
        export_decorator: str = "pycloud_export",
        dependency_allowlist: Optional[Sequence[str]] = None,
        chunk_size: int = 256 * 1024,
        healthy_only: bool = True,
        tags: Optional[Sequence[str]] = None,
        node_ids: Optional[Sequence[str]] = None,
        node_count: int = 0,
        node_limit: int = 100,
        require_credit: bool = True,
        preferred_runtime_key: str = "",
        timeout_sec: float = 10.0,
    ) -> "TaskModuleClient":
        """从 InfoCenter 创建 TaskModuleClient。

        参数与 TaskBatchClient.from_infocenter 相同。

        Returns:
            TaskModuleClient: 任务模块客户端
        """
        batch = TaskBatchClient.from_infocenter(
            infocenter_target=infocenter_target,
            client_id=client_id,
            job_id=job_id,
            code_version=code_version,
            artifact_path=artifact_path,
            blob=blob,
            filename=filename,
            runtime=runtime,
            entry_module=entry_module,
            entry_callable=entry_callable,
            package_format=package_format,
            export_mode=export_mode,
            export_methods=export_methods,
            export_decorator=export_decorator,
            dependency_allowlist=dependency_allowlist,
            chunk_size=chunk_size,
            healthy_only=healthy_only,
            tags=tags,
            node_ids=node_ids,
            node_count=node_count,
            node_limit=node_limit,
            require_credit=require_credit,
            preferred_runtime_key=preferred_runtime_key,
            timeout_sec=timeout_sec,
        )
        return cls(batch)

    def __getattr__(self, name: str):
        """动态创建任务调用代理。

        Args:
            name: 任务方法名（实际上会作为 entry_callable）

        Returns:
            _TaskCallProxy: 任务调用代理
        """
        # 避免无限递归
        if name.startswith("_"):
            raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")

        # 创建并返回任务调用代理
        return _TaskCallProxy(
            payload={},  # 初始 payload 为空，调用时提供
            batch=self._batch,
            timeout_hint_sec=0,
            priority=1,
            runtime_key="",
        )

    @property
    def client_id(self) -> str:
        """返回 client_id。"""
        return self._batch.client_id

    @property
    def job_id(self) -> str:
        """返回 job_id。"""
        return self._batch.job_id

    @property
    def code_version(self) -> str:
        """返回 code_version。"""
        return self._batch.code_version

    @property
    def nodes(self) -> Dict[str, InfoCenterNode]:
        """返回节点列表。"""
        return self._batch.nodes

    @property
    def node_ids(self) -> Sequence[str]:
        """返回节点 ID 列表。"""
        return self._batch.node_ids

    def submit_payloads(
        self,
        payloads: Sequence[Dict[str, object]],
        *,
        execution_mode: int = pb2.EXECUTION_MODE_PERSISTENT,
        job_id: str = "",
        task_id_prefix: str = "",
        timeout_hint_sec: int = 0,
        priority: int = 1,
        runtime_key: str = "",
    ) -> pb2.SubmitTasksResponse:
        """批量提交任务。

        Args:
            payloads: payload 列表
            execution_mode: 执行模式
            job_id: 作业 ID
            task_id_prefix: task_id 前缀
            timeout_hint_sec: 超时提示
            priority: 优先级
            runtime_key: 运行时键

        Returns:
            pb2.SubmitTasksResponse: 提交响应
        """
        return self._batch.submit_payloads(
            payloads,
            execution_mode=execution_mode,
            job_id=job_id,
            task_id_prefix=task_id_prefix,
            timeout_hint_sec=timeout_hint_sec,
            priority=priority,
            runtime_key=runtime_key,
        )

    def pull_results(
        self,
        *,
        limit: int = 100,
        wait_ms: int = 0,
        cursor: str = "",
    ) -> pb2.PullResultsResponse:
        """拉取结果。

        Args:
            limit: 最大结果数
            wait_ms: 等待时间（毫秒）
            cursor: 游标

        Returns:
            pb2.PullResultsResponse: 拉取响应
        """
        return self._batch.pull_results(limit=limit, wait_ms=wait_ms, cursor=cursor)

    def wait_for_results(
        self,
        *,
        expected_count: int = 0,
        timeout_sec: float = 30.0,
        wait_ms: int = 500,
        limit: int = 100,
        job_id: str = "",
    ) -> Sequence[pb2.TaskResult]:
        """等待结果。

        Args:
            expected_count: 期望结果数
            timeout_sec: 超时时间
            wait_ms: 等待间隔
            limit: 每次拉取限制
            job_id: 作业 ID

        Returns:
            Sequence[pb2.TaskResult]: 结果列表
        """
        return self._batch.wait_for_results(
            expected_count=expected_count,
            timeout_sec=timeout_sec,
            wait_ms=wait_ms,
            limit=limit,
            job_id=job_id,
        )

    def cancel_tasks(
        self,
        task_ids: Sequence[str],
        *,
        reason: str = "",
    ) -> pb2.CancelTasksResponse:
        """取消任务。

        Args:
            task_ids: 任务 ID 列表
            reason: 取消原因

        Returns:
            pb2.CancelTasksResponse: 取消响应
        """
        return self._batch.cancel_tasks(task_ids, reason=reason)

    def cancel_job(
        self,
        *,
        reason: str = "",
        job_id: str = "",
    ) -> pb2.CancelJobResponse:
        """取消作业。

        Args:
            reason: 取消原因
            job_id: 作业 ID

        Returns:
            pb2.CancelJobResponse: 取消响应
        """
        return self._batch.cancel_job(reason=reason, job_id=job_id)

    def get_metrics(self) -> Dict[str, pb2.GetMetricsResponse]:
        """获取节点指标。

        Returns:
            Dict[str, pb2.GetMetricsResponse]: 节点指标
        """
        return self._batch.get_metrics()

    def close(self) -> None:
        """关闭客户端。"""
        self._batch.close()

    def __enter__(self) -> "TaskModuleClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def __repr__(self) -> str:
        return (
            f"<TaskModuleClient "
            f"client_id={self.client_id!r} "
            f"job_id={self.job_id!r} "
            f"nodes={len(self.node_ids)}>"
        )


# ============================================================================
# 类别名（新命名，推荐使用）
# ============================================================================

# 新命名：DeployedService（部署并拥有服务）
DeployedService = ServiceModuleGroup

# 新命名：TaskSubmitter（提交任务）
TaskSubmitter = TaskModuleClient

# 新命名：GatewayConnect（通过网关连接）
GatewayConnect = GatewayModuleClient

# 新命名：DirectConnect（直接连接实例）
DirectConnect = DiscoveryModuleClient
