from __future__ import annotations

"""In-memory state backends for InfoCenter and NodeControl."""

import hashlib
import importlib
import importlib.util
import inspect
import multiprocessing as mp
import os
import queue
import secrets
import shutil
import sys
import tarfile
import tempfile
import threading
import uuid
import zipfile
from concurrent.futures import Future, ProcessPoolExecutor, TimeoutError as FutureTimeout
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Deque, Dict, Iterable, List, Optional, Sequence, Tuple

from google.protobuf import json_format
from google.protobuf import struct_pb2
from google.protobuf import timestamp_pb2

from pycloud_parallel.controlplane.http_gateway import ServiceHttpGateway
from pycloud_parallel.controlplane.hooks import InMemoryResultHook
from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2


def _normalize_user_return(ret: Any) -> Tuple[str, Optional[dict], str, str]:
    def _normalize_status(v: Any) -> str:
        s = str(v or "SUCCEEDED").strip().upper()
        if s in ("SUCCESS", "OK"):
            return "SUCCEEDED"
        if s not in ("SUCCEEDED", "FAILED_USER", "FAILED_INFRA"):
            return "SUCCEEDED"
        return s

    if isinstance(ret, tuple) and len(ret) == 4:
        status_text, result, err_type, err_message = ret
        return _normalize_status(status_text), result, str(err_type), str(err_message)

    if isinstance(ret, dict) and "status" in ret:
        status_text = _normalize_status(ret.get("status", "SUCCEEDED"))
        result = ret.get("result")
        err_type = str(ret.get("error_type", ""))
        err_message = str(ret.get("error_message", ""))
        return status_text, result, err_type, err_message

    if isinstance(ret, dict):
        return "SUCCEEDED", ret, "", ""
    return "SUCCEEDED", {"value": ret}, "", ""


_ROUTER_CACHE_LOCK = threading.Lock()
_ROUTER_CACHE: Dict[str, Tuple[Dict[str, Any], Dict[str, Tuple[str, str]]]] = {}


def _artifact_module_name(artifact_path: str) -> str:
    return f"_pycloud_user_{hashlib.sha1(artifact_path.encode('utf-8')).hexdigest()}"


def _normalize_package_format(package_format: str, filename: str) -> str:
    raw = str(package_format or "").strip().lower().replace("_", "").replace(".", "")
    if raw in ("py", "python"):
        return "py"
    if raw in ("targz", "tgz", "tar"):
        return "tar.gz"
    if raw == "zip":
        return "zip"
    if raw == "whl":
        return "whl"

    lower_name = str(filename or "").strip().lower()
    if lower_name.endswith(".tar.gz") or lower_name.endswith(".tgz"):
        return "tar.gz"
    if lower_name.endswith(".zip"):
        return "zip"
    if lower_name.endswith(".whl"):
        return "whl"
    if lower_name.endswith(".py"):
        return "py"
    return "bin"


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
    normalized_decorator = str(decorator or "").strip() or "pycloud_export"
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
    sys.path.insert(0, artifact_path)
    try:
        return importlib.import_module(entry_module)
    finally:
        try:
            sys.path.remove(artifact_path)
        except ValueError:
            pass


def _purge_loaded_artifact_modules(
    artifact_path: str,
    *,
    entry_module: str,
    package_format: str,
) -> None:
    format_name = _normalize_package_format(package_format, Path(artifact_path).name)
    if format_name == "py":
        _purge_module_tree(_artifact_module_name(artifact_path))
        return
    root_module = str(entry_module or "").split(".", 1)[0].strip()
    if root_module:
        _purge_module_tree(root_module)
    _purge_module_tree(str(entry_module or "").strip())


def _build_callable_router(
    module,
    *,
    mode: str,
    methods: Sequence[str],
    decorator: str,
    entry_callable: str,
) -> Tuple[Dict[str, Any], Dict[str, Tuple[str, str]]]:
    marker = str(decorator or "").strip() or "pycloud_export"
    marker_candidates = {
        marker,
        f"__{marker}__",
        "pycloud_export",
        "__pycloud_export__",
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
        )


def _invoke_user_callable(fn, payload: dict):
    try:
        signature = inspect.signature(fn)
        params = list(signature.parameters.values())
    except Exception:
        params = []

    if not params:
        return fn()

    if len(params) == 1 and params[0].kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD):
        return fn(payload)

    if isinstance(payload, dict):
        try:
            return fn(**payload)
        except TypeError:
            return fn(payload)
    return fn(payload)


def _execute_payload_in_subprocess(
    artifact_path: str,
    entry_module: str,
    package_format: str,
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
            export_mode=export_mode,
            export_methods=export_methods,
            export_decorator=export_decorator,
            entry_callable=entry_callable,
        )
        method = str(method_name or "").strip() or str(entry_callable or "run").strip() or "run"
        fn = router.get(method)
        if fn is None:
            raise RuntimeError(f"method `{method}` not exported")
        ret = _invoke_user_callable(fn, payload)
        return _normalize_user_return(ret)
    except Exception as exc:
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


def struct_to_dict(data: struct_pb2.Struct) -> dict:
    """将 protobuf Struct 转换为字典。

    Args:
        data: protobuf Struct 对象

    Returns:
        dict: 转换后的字典
    """
    return json_format.MessageToDict(data, preserving_proto_field_name=True)


def dict_to_struct(data: Optional[dict]) -> struct_pb2.Struct:
    """将字典转换为 protobuf Struct。

    Args:
        data: 输入字典

    Returns:
        struct_pb2.Struct: protobuf Struct 对象
    """
    out = struct_pb2.Struct()
    if data:
        out.update(data)
    return out


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
    metadata: Dict[str, str] = field(default_factory=dict)
    healthy: bool = True
    last_seen_at: datetime = field(default_factory=utc_now)
    metrics: NodeMetricsState = field(default_factory=NodeMetricsState)
    services: Dict[str, NodeServiceState] = field(default_factory=dict)
    service_worker_capacity: int = 0
    service_worker_used: int = 0
    schedulable: bool = True
    drain: bool = False
    reason: str = ""

    def service_worker_available(self) -> int:
        capacity = max(0, int(self.service_worker_capacity or 0))
        used = max(0, int(self.service_worker_used or 0))
        return max(0, capacity - used)


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
        service_worker_capacity: int = 0,
        service_worker_used: int = 0,
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
                )
                self._nodes[node_id] = state
            state.control_addr = control_addr
            state.capacity = max(1, capacity)
            state.queue_capacity = max(1, queue_capacity)
            state.tags = list(tags or [])
            state.version = str(version or "")
            state.metadata = dict(metadata or {})
            state.healthy = True
            state.last_seen_at = now
            state.services = dict(services or {})
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
            service_worker_capacity=int(metadata.get("service_worker_capacity", "0") or 0),
            service_worker_used=int(metadata.get("service_worker_used", "0") or 0),
        )

    def heartbeat_record(
        self,
        *,
        node_id: str,
        healthy: bool,
        metrics: Optional[NodeMetricsState] = None,
        services: Optional[Dict[str, NodeServiceState]] = None,
        service_worker_capacity: int = 0,
        service_worker_used: int = 0,
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
                        metadata=dict(state.metadata),
                        healthy=is_healthy,
                        last_seen_at=state.last_seen_at,
                        metrics=NodeMetricsState(**vars(state.metrics)),
                        services={k: NodeServiceState(**vars(v)) for k, v in state.services.items()},
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
                metadata=dict(state.metadata),
                healthy=state.healthy,
                last_seen_at=state.last_seen_at,
                metrics=NodeMetricsState(**vars(state.metrics)),
                services={k: NodeServiceState(**vars(v)) for k, v in state.services.items()},
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
    result: Optional[dict] = None
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
    executor: Optional[ProcessPoolExecutor] = None
    in_flight: int = 0
    queued: int = 0
    alive_workers: int = 0
    stop_reason: str = ""
    methods: Dict[str, Tuple[str, str]] = field(default_factory=dict)


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
        service_http_bind: str = "127.0.0.1:18080",
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
        self.service_http_bind = service_http_bind
        self.service_http_base_url = service_http_base_url.strip()
        self.started_at = utc_now()

        self._lock = threading.Lock()
        self._cv = threading.Condition(self._lock)
        self._pending: Deque[str] = deque()
        self._tasks: Dict[str, TaskState] = {}
        self._codes: Dict[str, CodeArtifact] = {}
        self._services: Dict[str, ServiceSession] = {}
        self._result_hook = InMemoryResultHook()

        self._artifact_dir = Path(artifact_dir)
        self._artifact_dir.mkdir(parents=True, exist_ok=True)

        self._stop_event = threading.Event()
        self._monitor = threading.Thread(target=self._monitor_loop, name="nodecontrol-monitor", daemon=True)
        self._monitor.start()
        self._done_queue: "queue.Queue[Future]" = queue.Queue()
        self._inflight_futures: Dict[Future, Tuple[str, int]] = {}
        self._internal_executor: Optional[ProcessPoolExecutor] = None
        self._dispatcher: Optional[threading.Thread] = None
        self._service_http_gateway: Optional[ServiceHttpGateway] = None

        if self.enable_internal_executor:
            mp_ctx = mp.get_context("spawn")
            self._internal_executor = ProcessPoolExecutor(max_workers=self.worker_capacity, mp_context=mp_ctx)
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
        if self._internal_executor is not None:
            self._internal_executor.shutdown(wait=True, cancel_futures=True)
        if self._service_http_gateway is not None:
            self._service_http_gateway.stop()
        self._shutdown_all_services()

    @property
    def artifact_dir(self) -> Path:
        return self._artifact_dir

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
        sha256: str,
        filename: str,
        runtime: str,
        entry_module: str,
        entry_callable: str,
        package_format: str = "",
        export_mode: str = "",
        export_methods: Sequence[str] = (),
        export_decorator: str = "",
        uploaded_path: str,
        actual_sha256: str,
        size_bytes: int,
    ) -> Tuple[CodeArtifact, bool]:
        expected = str(sha256 or "").replace("sha256:", "").strip().lower()
        digest = str(actual_sha256 or "").strip().lower()
        if not digest:
            raise ValueError("empty uploaded artifact")
        if expected and expected != digest:
            raise ValueError(f"sha256 mismatch: expected={expected}, actual={digest}")

        code_version = f"sha256:{digest}"
        with self._lock:
            existing = self._codes.get(code_version)
            if existing is not None:
                return existing, True

        normalized_format = _normalize_package_format(package_format, filename)
        normalized_callable = str(entry_callable or "").strip() or "run"
        normalized_module = str(entry_module or "").strip()
        if not normalized_module and normalized_format == "py":
            normalized_module = Path(filename).stem
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
        if normalized_format == "py":
            final_path = self._artifact_dir / f"{digest}.py"
            os.replace(str(tmp_path), str(final_path))
            artifact_exec_path = str(final_path)
        else:
            ext = "tar.gz" if normalized_format == "tar.gz" else normalized_format
            archive_path = self._artifact_dir / f"{digest}.{ext}"
            os.replace(str(tmp_path), str(archive_path))
            extract_dir = self._artifact_dir / f"{digest}_pkg"
            self._extract_archive(archive_path=archive_path, package_format=normalized_format, out_dir=extract_dir)
            artifact_exec_path = str(extract_dir)

        artifact = CodeArtifact(
            code_version=code_version,
            path=artifact_exec_path,
            runtime=str(runtime or "").strip(),
            entry_module=normalized_module,
            entry_callable=normalized_callable,
            package_format=normalized_format,
            export_mode=normalized_export_mode,
            export_methods=normalized_export_methods,
            export_decorator=normalized_export_decorator,
            size_bytes=max(0, int(size_bytes)),
            created_at=now,
        )
        with self._lock:
            self._codes[code_version] = artifact
        return artifact, False

    def put_code(
        self,
        *,
        sha256: str,
        filename: str,
        runtime: str,
        entry_module: str,
        entry_callable: str,
        package_format: str = "",
        export_mode: str = "",
        export_methods: Optional[Sequence[str]] = None,
        export_decorator: str = "",
        chunks: Iterable[bytes],
    ) -> Tuple[CodeArtifact, bool]:
        h = hashlib.sha256()
        size = 0
        suffix = ".tar.gz" if str(filename or "").lower().endswith(".tar.gz") else (Path(filename).suffix or ".bin")
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
                sha256=sha256,
                filename=filename,
                runtime=runtime,
                entry_module=entry_module,
                entry_callable=entry_callable,
                package_format=package_format,
                export_mode=export_mode,
                export_methods=list(export_methods or ()),
                export_decorator=export_decorator,
                uploaded_path=str(tmp_path),
                actual_sha256=h.hexdigest(),
                size_bytes=size,
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
        filename: str,
        sha256: str,
        runtime: str,
        entry_module: str,
        entry_callable: str,
        package_format: str = "",
        export_mode: str = "",
        export_methods: Sequence[str] = (),
        export_decorator: str = "",
        worker_count: int,
        heartbeat_timeout_sec: int,
        idle_ttl_sec: int,
        expose_http: bool,
        chunks: Iterable[bytes],
    ) -> ServiceSession:
        if not owner_client_id:
            raise ValueError("owner_client_id is required")

        artifact, _cached = self.put_code(
            sha256=sha256,
            filename=filename or "service_artifact.py",
            runtime=runtime,
            entry_module=entry_module,
            entry_callable=entry_callable,
            package_format=package_format,
            export_mode=export_mode,
            export_methods=export_methods,
            export_decorator=export_decorator,
            chunks=chunks,
        )
        method_info = _discover_callable_methods(
            artifact.path,
            entry_module=artifact.entry_module,
            package_format=artifact.package_format,
            export_mode=artifact.export_mode,
            export_methods=artifact.export_methods,
            export_decorator=artifact.export_decorator,
            entry_callable=artifact.entry_callable,
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

        mp_ctx = mp.get_context("spawn")
        executor = ProcessPoolExecutor(max_workers=actual_workers, mp_context=mp_ctx)
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
            executor=executor,
            alive_workers=actual_workers,
            methods=method_info,
        )
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
        executor = session.executor
        session.executor = None
        session.stop_reason = reason
        session.alive_workers = 0
        session.status = pb2.SERVICE_STATUS_STOPPED
        session.lease_expire_at = utc_now()
        if executor is not None:
            executor.shutdown(wait=False, cancel_futures=True)

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
            executor = session.executor
            if executor is None:
                return 409, {"ok": False, "error": "service executor stopped"}
            session.in_flight += 1

        try:
            future = executor.submit(
                _execute_payload_in_subprocess,
                artifact.path,
                artifact.entry_module,
                artifact.package_format,
                artifact.export_mode,
                artifact.export_methods,
                artifact.export_decorator,
                requested_method,
                artifact.entry_callable,
                payload or {},
            )
            status_text, result, err_type, err_message = future.result(timeout=max(0.1, timeout_sec))
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

    def _on_future_done(self, future: Future) -> None:
        self._done_queue.put(future)

    def _touch_internal_heartbeats_locked(self) -> None:
        now = utc_now()
        for task_id, attempt in self._inflight_futures.values():
            task = self._tasks.get(task_id)
            if task is None:
                continue
            if task.attempt != attempt:
                continue
            if task.status != pb2.TASK_STATUS_RUNNING:
                continue
            task.last_heartbeat_at = now

    def _drain_completed_futures(self) -> None:
        while True:
            try:
                future = self._done_queue.get_nowait()
            except queue.Empty:
                return

            with self._cv:
                meta = self._inflight_futures.pop(future, None)
            if meta is None:
                continue
            task_id, attempt = meta

            try:
                status_text, result, err_type, err_message = future.result()
            except Exception as exc:
                status_text = "FAILED_INFRA"
                result = None
                err_type = "InfraException"
                err_message = repr(exc)

            now = utc_now()
            with self._cv:
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
                    task.result = result or {}
                    task.error_type = ""
                    task.error_message = ""
                    self._publish_result_locked(task)
                self._cv.notify_all()

    def _dispatch_loop(self) -> None:
        while not self._stop_event.is_set():
            self._drain_completed_futures()
            with self._cv:
                self._touch_internal_heartbeats_locked()
                if self._internal_executor is not None:
                    while len(self._inflight_futures) < self.worker_capacity:
                        slot = len(self._inflight_futures) + 1
                        envelope = self._claim_task_locked(worker_id=f"local-proc-{slot:02d}")
                        if envelope is None:
                            break
                        artifact = self._codes.get(envelope.code_version)
                        if artifact is None:
                            task = self._tasks.get(envelope.task_id)
                            if task is not None:
                                now = utc_now()
                                self._handle_infra_failure_locked(task, reason="missing code artifact", now=now)
                            continue
                        future = self._internal_executor.submit(
                            _execute_payload_in_subprocess,
                            artifact.path,
                            artifact.entry_module,
                            artifact.package_format,
                            artifact.export_mode,
                            artifact.export_methods,
                            artifact.export_decorator,
                            artifact.entry_callable,
                            artifact.entry_callable,
                            struct_to_dict(envelope.payload),
                        )
                        self._inflight_futures[future] = (envelope.task_id, envelope.attempt)
                        future.add_done_callback(self._on_future_done)
            self._drain_completed_futures()
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
