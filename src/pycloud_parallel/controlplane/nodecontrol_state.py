from __future__ import annotations

"""NodeControl state backend."""

import contextlib
import hashlib
import inspect
import json
import logging
import os
import secrets
import shutil
import stat
import sys
import tarfile
import tempfile
import threading
import time
import uuid
import zipfile
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import unquote

from pycloud_parallel.controlplane.artifact import (
    _dependency_policy_allows_install,
    _normalize_dependency_policy_mode,
)
from pycloud_parallel.controlplane.config import (
    OBJECT_SEGMENT_MAX_BYTES,
    OBJECT_SEGMENT_TARGET_BYTES,
    get_payload_policy,
)
from pycloud_parallel.controlplane.data_store import DataStore
from pycloud_parallel.controlplane.node_capability import NodeCapability, detect_local_node_capability
from pycloud_parallel.controlplane.executor_host import ExecutorHostClient
from pycloud_parallel.controlplane.hooks import InMemoryResultHook
from pycloud_parallel.controlplane.code_version import _code_version_from_digest
from pycloud_parallel.controlplane.infocenter.models import NodeTaskPoolInfo
from pycloud_parallel.controlplane.node_runtime_base import NodeRuntimeBase
from pycloud_parallel.controlplane.node.helpers import (
    _append_bytes_to_segment,
    _artifact_exists,
    _build_execute_spec,
    _cleanup_orphan_segment_file,
    _code_archive_path,
    _code_artifact_exists,
    _code_content_dir,
    _code_content_storage_key,
    _code_data_dir,
    _code_dependency_dir,
    _code_exec_path,
    _code_globals_dir,
    _code_variant_dir,
    _describe_artifact_error,
    _discover_callable_methods,
    _install_dependency_allowlist,
    _is_user_artifact_error,
    _load_managed_globals_snapshot_serialized,
    _load_object_meta,
    _managed_globals_binary_value,
    _managed_globals_scope_dir,
    _missing_import_name,
    _normalize_dependency_allowlist,
    _normalize_export_spec,
    _normalize_managed_global_names,
    _normalize_package_format,
    _object_artifact_from_meta,
    _object_meta_path,
    _package_suffix,
    _pin_object_meta,
    _purge_loaded_artifact_modules,
    _release_object_meta_pin,
    _resolve_apply_managed_globals_hook,
    _resolve_single_data_ref,
    _segment_path_from_relpath,
    _segment_relpath,
    _store_result_dataframe,
    _store_result_ndarray,
    _store_result_path,
    _store_result_series,
    _validate_python_runtime_or_raise,
    _write_code_meta,
    _write_managed_globals_current,
    _write_managed_globals_snapshot,
    _write_object_meta,
    service_timing_logger,
    touch_code_last_at,
)
from pycloud_parallel.controlplane.node.models import (
    CodeArtifact,
    ManagedGlobalsState,
    ObjectArtifact,
    ServiceSession,
    StoredResultArtifact,
    TaskPoolState,
    TaskState,
)
from pycloud_parallel.controlplane.node.session_views import (
    build_service_report_payload as _build_service_report_payload,
    build_service_route_report as _build_service_route_report,
    build_service_status_info as _build_service_status_info,
    build_task_pool_info as _build_task_pool_info,
    build_task_pool_status_info as _build_task_pool_status_info,
    execute_warmup as _execute_session_warmup,
    log_warmup_result as _log_session_warmup_result,
    normalize_warmup_result as _normalize_session_warmup_result,
    warmup_fanout as _session_warmup_fanout,
)
from pycloud_parallel.controlplane.node.timing import (
    ExecutionTimingSample,
    record_execution_timing,
)
from pycloud_parallel.data.ref import (
    normalize_object_format,
    normalize_object_id,
    object_id_from_sha256_hex,
    object_storage_path,
)
from pycloud_parallel.controlplane.payload_transport import decode_payload_from_transport
from pycloud_parallel.controlplane.serialization import (
    decode_transport_payload_bytes,
    detect_transport_mode,
    log_payload_flow,
    serialize_arrow_compatible,
    stable_pickle_dumps,
    struct_to_python,
)
from pycloud_parallel.controlplane.state_time import dt_to_ts, utc_now
from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2
from pycloud_parallel.runtime.errors import normalize_invoke_error


logger = logging.getLogger(__name__)


class NodeControlState(NodeRuntimeBase):
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
        super().__init__(
            node_id=node_id,
            service_http_bind=service_http_bind,
            service_http_base_url=service_http_base_url,
            accept_service_deploy=True,
        )
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
        self.started_at = utc_now()

        self._lock = threading.Lock()
        self._cv = threading.Condition(self._lock)
        self._pool_tasks: Dict[str, TaskState] = {}
        self._pool_task_reserved_ids: set[str] = set()
        self._codes: Dict[str, CodeArtifact] = {}
        self._objects: Dict[str, ObjectArtifact] = {}
        self._services: Dict[str, ServiceSession] = {}
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
        segment_max_bytes = max(0, int(OBJECT_SEGMENT_MAX_BYTES))
        segment_target_bytes = int(OBJECT_SEGMENT_TARGET_BYTES)
        self._object_segment_max_bytes = segment_max_bytes
        self._object_segment_target_bytes = max(segment_max_bytes, segment_target_bytes)

        self._stop_event = threading.Event()
        self._monitor = threading.Thread(target=self._monitor_loop, name="nodecontrol-monitor", daemon=True)
        self._monitor.start()
        self._executor_host = (
            ExecutorHostClient(task_worker_capacity=self.worker_capacity)
            if (self.enable_internal_executor or self.enable_service_session)
            else None
        )
        self._cleanup_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="nodecontrol-cleanup")
        self._dispatcher: Optional[threading.Thread] = None

        if self.enable_internal_executor:
            self._dispatcher = threading.Thread(
                target=self._dispatch_loop,
                name="nodecontrol-dispatcher",
                daemon=True,
            )
            self._dispatcher.start()

        if self.enable_service_session and self.service_http_bind:
            self.start_service_gateway(
                invoke_handler=self._invoke_service_http,
                status_handler=self._service_status_http,
                methods_handler=self._service_methods_http,
                extra_get_handler=self._service_extra_get_http,
            )

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
            sample = ExecutionTimingSample(
                method=str(method or ""),
                ok=bool(ok),
                http_status=int(http_status or 0),
                setup_ms=float(setup_ms or 0.0),
                build_execute_spec_ms=float(build_execute_spec_ms or 0.0),
                executor_ms=float(executor_ms or 0.0),
                finalize_ms=float(finalize_ms or 0.0),
                total_ms=float(total_ms or 0.0),
                subprocess_timings=dict(subprocess_timings or {}) or None,
                error_type=str(error_type or ""),
                error_message=str(error_message or ""),
            )
            session.timing_metrics = record_execution_timing(
                session.timing_metrics,
                sample=sample,
                include_http_status=True,
                include_queue_wait=True,
                updated_at=utc_now().isoformat(),
                event="service_timing",
                id_key="service_id",
                id_value=session.service_id,
                name_key="service_name",
                name_value=session.service_name,
                logger=service_timing_logger,
            )
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
            sample = ExecutionTimingSample(
                method=str(method or ""),
                ok=bool(ok),
                setup_ms=float(setup_ms or 0.0),
                build_execute_spec_ms=float(build_execute_spec_ms or 0.0),
                executor_ms=float(executor_ms or 0.0),
                finalize_ms=float(finalize_ms or 0.0),
                total_ms=float(total_ms or 0.0),
                subprocess_timings=dict(subprocess_timings or {}) or None,
                error_type=str(error_type or ""),
                error_message=str(error_message or ""),
            )
            pool.timing_metrics = record_execution_timing(
                pool.timing_metrics,
                sample=sample,
                include_http_status=False,
                include_queue_wait=True,
                updated_at=utc_now().isoformat(),
                event="task_pool_timing",
                id_key="pool_id",
                id_value=pool.pool_id,
                name_key="pool_name",
                name_value=pool.pool_name,
                logger=service_timing_logger,
            )
        except Exception as exc:
            logger = logging.getLogger(__name__)
            logger.debug("failed to record task pool timing: %r", exc)

    def _record_task_pool_lifecycle_timing_locked(
        self,
        pool: TaskPoolState,
        *,
        metric: str,
        elapsed_ms: float,
    ) -> None:
        metrics = dict(pool.timing_metrics or {})
        count_key = f"{metric}_count"
        last_key = f"last_{metric}_ms"
        avg_key = f"avg_{metric}_ms"
        count = int(metrics.get(count_key, 0) or 0) + 1
        metrics[count_key] = count
        metrics[last_key] = round(float(elapsed_ms or 0.0), 3)
        prev_avg = float(metrics.get(avg_key, 0.0) or 0.0)
        metrics[avg_key] = round((((prev_avg * (count - 1)) + float(elapsed_ms or 0.0)) / count), 3)
        metrics["updated_at"] = utc_now().isoformat()
        pool.timing_metrics = metrics

    def _increment_task_pool_metric_locked(
        self,
        pool: TaskPoolState,
        *,
        key: str,
        delta: int = 1,
    ) -> None:
        metrics = dict(pool.timing_metrics or {})
        metrics[str(key)] = int(metrics.get(str(key), 0) or 0) + int(delta or 0)
        metrics["updated_at"] = utc_now().isoformat()
        pool.timing_metrics = metrics

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
        self.stop_service_gateway()
        self._shutdown_all_services()
        self._cleanup_executor.shutdown(wait=False, cancel_futures=True)
        if self._executor_host is not None:
            self._executor_host.close(shutdown_timeout_sec=2.0)

    def _submit_stop_task_pool(self, executor_host: ExecutorHostClient, *, pool_id: str) -> None:
        normalized = str(pool_id or "").strip()
        if not normalized:
            return

        def _stop() -> None:
            try:
                executor_host.stop_task_pool(pool_id=normalized)
            except Exception:
                logger.exception("[NodeControl] async stop_task_pool failed pool_id=%s", normalized)

        try:
            self._cleanup_executor.submit(_stop)
        except RuntimeError:
            _stop()

    @property
    def data_store(self) -> DataStore:
        object_dir = str(self._object_dir)
        return DataStore(
            object_dir=object_dir,
            node_id=str(self.node_id or ""),
            control_addr="",
            put_uploaded_file_impl=self.put_object_from_uploaded_file,
            store_path_impl=lambda path: _store_result_path(path, object_dir=object_dir),
            store_dataframe_impl=lambda frame: _store_result_dataframe(frame, object_dir=object_dir),
            store_series_impl=lambda series: _store_result_series(series, object_dir=object_dir),
            store_ndarray_impl=lambda array: _store_result_ndarray(array, object_dir=object_dir),
            register_stored_result_impl=self._register_stored_result_artifact_locked,
            resolve_data_ref_impl=lambda ref: _resolve_single_data_ref(ref, object_dir=object_dir),
        )

    def node_capability(self) -> NodeCapability:
        return detect_local_node_capability()

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
        serialization_mode: str = "",
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
            if str(serialization_mode or "").strip().lower() == "pickle_stable_v1":
                current_values[name] = _managed_globals_binary_value(
                    codec="pickle_stable_v1",
                    payload=stable_pickle_dumps(prepared_value),
                )
            else:
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
        return _session_warmup_fanout(worker_count)

    @staticmethod
    def _normalize_warmup_result(result: object, *, fanout: int) -> Tuple[int, List[int]]:
        return _normalize_session_warmup_result(result, fanout=fanout)

    def _log_warmup_result(self, *, scope: str, key: str, worker_count: int, submitted_count: int, worker_pids: Sequence[int]) -> None:
        _log_session_warmup_result(
            logger=logging.getLogger(__name__),
            scope=scope,
            key=key,
            worker_count=worker_count,
            submitted_count=submitted_count,
            worker_pids=worker_pids,
        )

    def _execute_warmup(
        self,
        *,
        executor_host: ExecutorHostClient,
        scope: str,
        key: str,
        worker_count: int,
        execute_spec: Dict[str, object],
    ) -> Tuple[int, List[int]]:
        submitted, worker_pids = _execute_session_warmup(
            executor_host,
            scope=scope,
            key=key,
            worker_count=worker_count,
            execute_spec=execute_spec,
        )
        self._log_warmup_result(
            scope=scope,
            key=key,
            worker_count=worker_count,
            submitted_count=submitted,
            worker_pids=worker_pids,
        )
        return submitted, worker_pids

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
        segment_relpath = ""
        if artifact.storage_backend == "segment" and artifact.segment_path:
            with contextlib.suppress(Exception):
                segment_relpath = _segment_relpath(self._object_dir, Path(artifact.segment_path))
        if artifact.storage_backend != "segment" and artifact.path:
            with contextlib.suppress(FileNotFoundError):
                Path(artifact.path).unlink()
        with contextlib.suppress(FileNotFoundError):
            _object_meta_path(self._object_dir, object_id=object_id).unlink()
        if segment_relpath:
            _cleanup_orphan_segment_file(self._object_dir, segment_relpath=segment_relpath)

    def pin_object(self, object_id: str, *, ref_id: str) -> bool:
        normalized = normalize_object_id(object_id)
        normalized_ref_id = str(ref_id or "").strip()
        if not normalized_ref_id:
            raise ValueError("ref_id is required")
        with self._lock:
            artifact = self._objects.get(normalized)
            fallback_path = None
            if artifact is None:
                meta = _load_object_meta(self._object_dir, object_id=normalized)
                if meta:
                    artifact = _object_artifact_from_meta(self._object_dir, object_id=normalized, meta=meta)
                    self._objects[normalized] = artifact
            if artifact is not None:
                fallback_path = Path(artifact.path) if artifact.path else Path(artifact.segment_path or "")
            return _pin_object_meta(self._object_dir, object_id=normalized, ref_id=normalized_ref_id, fallback_path=fallback_path)

    def release_object(self, object_id: str, *, ref_id: str = "") -> bool:
        normalized = normalize_object_id(object_id)
        normalized_ref_id = str(ref_id or "").strip()
        with self._lock:
            existing = self._objects.get(normalized)
            if existing is None:
                meta = _load_object_meta(self._object_dir, object_id=normalized)
                if not meta:
                    return False
                existing = _object_artifact_from_meta(self._object_dir, object_id=normalized, meta=meta)
                self._objects[normalized] = existing
            if normalized_ref_id:
                found, still_pinned = _release_object_meta_pin(self._object_dir, object_id=normalized, ref_id=normalized_ref_id)
                if not found:
                    return False
                if still_pinned:
                    return True
            self._delete_object_artifact_locked(normalized)
        return True

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
        for pool in self._task_pools.values():
            if not pool.is_running() or not pool.executor_ready:
                continue
            try:
                self._executor_host.create_task_pool(
                    pool_id=pool.pool_id,
                    worker_count=pool.worker_count,
                )
                pool.alive_workers = pool.worker_count
            except Exception:
                pool.executor_ready = False
                pool.alive_workers = 0
                pool.status = "STOPPED"
                pool.lease_expire_at = current_time

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
        if _resolve_apply_managed_globals_hook(module) is not None:
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
            extracted_dir = str(getattr(module, "__pycloud_temp_extract_dir__", "") or "").strip() if module is not None else ""
            _purge_loaded_artifact_modules(
                artifact.path,
                entry_module=artifact.entry_module,
                package_format=artifact.package_format,
                dependency_path=dependency_path,
                extra_prefixes=([extracted_dir] if extracted_dir else []),
            )

    def _ensure_artifact_ready(
        self,
        artifact: CodeArtifact,
        *,
        dependency_policy_mode: str = "",
        dependency_allowlist: Sequence[str],
        managed_global_names: Sequence[str] = (),
    ) -> Dict[str, Tuple[str, str]]:
        normalized_allowlist = _normalize_dependency_allowlist(dependency_allowlist)
        normalized_policy_mode = _normalize_dependency_policy_mode(
            dependency_policy_mode or artifact.dependency_policy_mode,
            dependency_allowlist=normalized_allowlist or artifact.dependency_allowlist,
        )
        installed_dependency_path = str(artifact.dependency_path or "").strip()
        effective_allowlist = (
            _normalize_dependency_allowlist([*artifact.dependency_allowlist, *normalized_allowlist])
            if _dependency_policy_allows_install(normalized_policy_mode)
            else ()
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
                            dependency_policy_mode=normalized_policy_mode,
                            install_failed=True,
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
                            dependency_policy_mode=normalized_policy_mode,
                        )
                    ) from exc
                raise

            if not effective_allowlist:
                if created_dir:
                    shutil.rmtree(candidate_dependency_path, ignore_errors=True)
                if _is_user_artifact_error(exc):
                    raise ValueError(
                        _describe_artifact_error(
                            exc,
                            entry_module=artifact.entry_module,
                            entry_callable=artifact.entry_callable,
                            package_format=artifact.package_format,
                            dependency_policy_mode=normalized_policy_mode,
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
                            dependency_policy_mode=normalized_policy_mode,
                            install_failed=True,
                        )
                    ) from repair_exc
                raise

            artifact.dependency_policy_mode = normalized_policy_mode
            artifact.dependency_allowlist = effective_allowlist
            artifact.dependency_path = str(target_dir)
            _write_code_meta(self._artifact_dir, artifact)
            return method_info

        artifact.dependency_policy_mode = normalized_policy_mode
        if effective_allowlist and candidate_dependency_path:
            artifact.dependency_allowlist = effective_allowlist
            artifact.dependency_path = candidate_dependency_path
            _write_code_meta(self._artifact_dir, artifact)
        elif artifact.dependency_path and not effective_allowlist:
            artifact.dependency_path = ""
            artifact.dependency_allowlist = ()
            _write_code_meta(self._artifact_dir, artifact)
        return method_info

    def service_worker_used(self) -> int:
        with self._lock:
            active = sum(
                session.resource_snapshot().worker_count
                for session in self._services.values()
                if session.is_running()
            )
            return active + max(0, int(self._service_worker_reserved))

    def service_worker_available(self) -> int:
        return max(0, int(self.service_worker_capacity) - int(self.service_worker_used()))

    @staticmethod
    def _service_inflight_locked(session: ServiceSession) -> int:
        return session.resource_snapshot().in_flight

    @staticmethod
    def _task_pool_inflight_locked(pool: TaskPoolState) -> int:
        return pool.resource_snapshot().in_flight

    @staticmethod
    def _pool_task_is_terminal_status(status: int) -> bool:
        return int(status) in {
            int(pb2.TASK_STATUS_SUCCEEDED),
            int(pb2.TASK_STATUS_FAILED_USER),
            int(pb2.TASK_STATUS_FAILED_INFRA),
            int(pb2.TASK_STATUS_CANCELLED),
        }

    @staticmethod
    def _pool_task_is_active_status(status: int) -> bool:
        return int(status) in {
            int(pb2.TASK_STATUS_QUEUED),
            int(pb2.TASK_STATUS_RUNNING),
        }

    def _node_queue_occupancy_locked(self) -> int:
        service_inflight = sum(self._service_inflight_locked(session) for session in self._services.values())
        pool_active = sum(
            1 for task in self._pool_tasks.values() if self._pool_task_is_active_status(int(task.status or 0))
        )
        return service_inflight + pool_active + len(self._pool_task_reserved_ids)

    def task_pool_worker_used(self) -> int:
        with self._lock:
            active = sum(
                pool.resource_snapshot().worker_count
                for pool in self._task_pools.values()
                if pool.is_running()
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
            reports: Dict[str, NodeTaskPoolInfo] = {}
            for pool in self._task_pools.values():
                if not (pool.is_running() or bool(pool.timing_metrics)):
                    continue
                reports[pool.pool_id] = _build_task_pool_info(
                    pool,
                    in_flight=inflight_by_pool.get(pool.pool_id, self._task_pool_inflight_locked(pool)),
                )
            return reports

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
        return _build_task_pool_status_info(pool, in_flight=inflight)

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

        def _apply_mode(path: Path, mode: int) -> None:
            normalized_mode = int(mode or 0) & 0o777
            if not normalized_mode:
                return
            with contextlib.suppress(OSError):
                path.chmod(normalized_mode)

        def _reject_unsupported_tar_member(member: tarfile.TarInfo) -> None:
            if member.issym() or member.islnk():
                raise ValueError(f"tar archive contains unsupported link entry: {member.name}")
            if member.isdev() or member.ischr() or member.isblk() or member.isfifo():
                raise ValueError(f"tar archive contains unsupported special entry: {member.name}")

        def _zip_info_mode(info: zipfile.ZipInfo) -> int:
            return (int(info.external_attr or 0) >> 16) & 0o777

        def _zip_info_is_symlink(info: zipfile.ZipInfo) -> bool:
            file_type = ((int(info.external_attr or 0) >> 16) & 0o170000)
            return file_type == stat.S_IFLNK

        if package_format in ("zip", "whl"):
            with zipfile.ZipFile(archive_path, "r") as zf:
                for info in zf.infolist():
                    target = _safe_join(info.filename)
                    if _zip_info_is_symlink(info):
                        raise ValueError(f"zip archive contains unsupported link entry: {info.filename}")
                    if info.is_dir():
                        target.mkdir(parents=True, exist_ok=True)
                        _apply_mode(target, _zip_info_mode(info))
                        continue
                    target.parent.mkdir(parents=True, exist_ok=True)
                    with zf.open(info, "r") as src, target.open("wb") as dst:
                        shutil.copyfileobj(src, dst)
                    _apply_mode(target, _zip_info_mode(info))
            return

        if package_format == "tar.gz":
            with tarfile.open(archive_path, "r:gz") as tf:
                for member in tf.getmembers():
                    target = _safe_join(member.name)
                    _reject_unsupported_tar_member(member)
                    if member.isdir():
                        target.mkdir(parents=True, exist_ok=True)
                        _apply_mode(target, member.mode)
                        continue
                    if not member.isfile():
                        raise ValueError(f"tar archive contains unsupported member type: {member.name}")
                    target.parent.mkdir(parents=True, exist_ok=True)
                    extracted = tf.extractfile(member)
                    if extracted is None:
                        raise ValueError(f"tar archive member could not be read: {member.name}")
                    with extracted, target.open("wb") as dst:
                        shutil.copyfileobj(extracted, dst)
                    _apply_mode(target, member.mode)
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
        dependency_policy_mode: str = "",
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
        normalized_dependency_policy_mode = _normalize_dependency_policy_mode(
            dependency_policy_mode,
            dependency_allowlist=normalized_dependency_allowlist,
        )
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
            dependency_policy_mode=normalized_dependency_policy_mode,
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
                            dependency_policy_mode=normalized_dependency_policy_mode,
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
                dependency_policy_mode=normalized_dependency_policy_mode,
                dependency_allowlist=normalized_dependency_allowlist,
                dependency_path="",
                size_bytes=max(0, int(size_bytes)),
                created_at=now,
            )
            if validate_load:
                try:
                    self._ensure_artifact_ready(
                        artifact,
                        dependency_policy_mode=normalized_dependency_policy_mode,
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
        dependency_policy_mode: str = "",
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
                dependency_policy_mode=dependency_policy_mode,
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
        dependency_policy_mode: str = "",
        dependency_allowlist: Sequence[str] = (),
        managed_global_names: Sequence[str] = (),
        policy_id: str = "",
        worker_count: int,
        heartbeat_timeout_sec: int,
        idle_ttl_sec: int,
        expose_http: bool,
        chunks: Iterable[bytes],
        service_id: str = "",
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
            dependency_policy_mode=dependency_policy_mode,
            dependency_allowlist=dependency_allowlist,
            chunks=chunks,
            validate_load=True,
        )
        method_info = self._ensure_artifact_ready(
            artifact,
            dependency_policy_mode=dependency_policy_mode,
            dependency_allowlist=dependency_allowlist,
            managed_global_names=normalized_managed_global_names,
        )

        requested_workers = max(1, worker_count or self.service_default_worker_count)
        actual_hb_timeout = max(5, heartbeat_timeout_sec or self.service_default_heartbeat_timeout_sec)
        actual_idle_ttl = max(0, idle_ttl_sec)
        now = utc_now()
        service_id = str(service_id or "").strip() or uuid.uuid4().hex
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
            executor_host.preload_service(
                service_id=service_id,
                fanout=actual_workers,
                execute_spec=_build_execute_spec(
                    artifact,
                    object_dir=self._object_dir,
                    work_dir=_code_data_dir(self._artifact_dir, code_version=artifact.code_version),
                    method_name=next(iter(method_info.keys()), artifact.entry_callable),
                    payload={},
                    payload_mode="http_call",
                    warmup_only=True,
                ),
            )
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
                policy_id=str(policy_id or "").strip().lower() or "default_safe",
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
            with contextlib.suppress(Exception):
                executor_host.stop_service(service_id=service_id)
            raise

    def update_globals(
        self,
        values: Dict[str, Any],
        *,
        service_id: str = "",
        service_name: str = "",
    ) -> str:
        """Update startup-created service globals through the normal service path."""
        if not isinstance(values, dict):
            raise RuntimeError("update_globals values must be a dict")
        normalized_service_id = str(service_id or "").strip()
        normalized_service_name = str(service_name or "").strip()
        with self._lock:
            sessions = [
                session
                for session in self._services.values()
                if (not normalized_service_id or session.service_id == normalized_service_id)
                and (not normalized_service_name or session.service_name == normalized_service_name)
            ]
        if not sessions:
            raise KeyError("service not found")
        digests: Dict[str, str] = {}
        for session in sessions:
            digest, _updated = self.update_service_globals(
                owner_client_id=session.owner_client_id,
                service_id=session.service_id,
                service_token=session.service_token,
                values=values,
            )
            digests[session.service_id] = digest
        self.globals_digests = dict(digests)
        unique = {digest for digest in digests.values() if str(digest).strip()}
        return next(iter(unique), "") if len(unique) == 1 else next(iter(digests.values()), "")

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
        dependency_policy_mode: str = "",
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
            dependency_policy_mode=dependency_policy_mode,
            dependency_allowlist=dependency_allowlist,
            chunks=chunks,
            validate_load=True,
        )
        self._ensure_artifact_ready(
            artifact,
            dependency_policy_mode=dependency_policy_mode,
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
        executor_create_started = time.monotonic()
        try:
            executor_host.create_task_pool(pool_id=pool_id, worker_count=actual_workers)
            executor_host.preload_pool(
                pool_id=pool_id,
                fanout=actual_workers,
                execute_spec=_build_execute_spec(
                    artifact,
                    object_dir=self._object_dir,
                    work_dir=_code_data_dir(self._artifact_dir, code_version=artifact.code_version),
                    method_name=str(entry_callable or "run").strip() or "run",
                    payload={},
                    payload_mode="task_submit",
                    warmup_only=True,
                ),
            )
        except Exception:
            with self._lock:
                self._task_pool_worker_reserved = max(0, self._task_pool_worker_reserved - reserved)
            with contextlib.suppress(Exception):
                executor_host.stop_task_pool(pool_id=pool_id)
            raise
        executor_create_ms = (time.monotonic() - executor_create_started) * 1000.0
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
                alive_workers=actual_workers,
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
                self._record_task_pool_lifecycle_timing_locked(
                    pool,
                    metric="executor_create",
                    elapsed_ms=executor_create_ms,
                )
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
        normalized_pool_id = str(pool_id or "").strip()
        normalized_job_id = str(job_id or "").strip()
        reserved_items: List[pb2.TaskSubmitItem] = []
        with self._cv:
            pool = self._task_pools.get(normalized_pool_id)
            if pool is None:
                raise KeyError("task pool not found")
            if not pool.pool_token or pool.pool_token != str(pool_token or "").strip():
                raise PermissionError("pool_token mismatch")
            if pool.status != "RUNNING":
                raise RuntimeError("task pool not running")
            artifact = self._codes.get(pool.code_version)
            if artifact is None:
                raise RuntimeError("code artifact missing")
            if self._executor_host is None:
                raise RuntimeError("executor host unavailable")
            for item in tasks:
                task_id = str(item.task_id or "").strip()
                if task_id in self._pool_tasks or task_id in self._pool_task_reserved_ids:
                    rejected.append(
                        pb2.TaskRejected(
                            task_id=task_id,
                            code=pb2.ERROR_CODE_DUPLICATE_TASK,
                            message="duplicate task_id",
                        )
                    )
                    continue
                if self._node_queue_occupancy_locked() >= int(self.queue_capacity):
                    rejected.append(
                        pb2.TaskRejected(
                            task_id=task_id,
                            code=pb2.ERROR_CODE_QUEUE_FULL,
                            message=(
                                f"node queue capacity exceeded "
                                f"(queue_capacity={int(self.queue_capacity)}, pool_id={normalized_pool_id})"
                            ),
                        )
                    )
                    continue
                self._pool_task_reserved_ids.add(task_id)
                reserved_items.append(item)

        work_dir = _code_data_dir(self._artifact_dir, code_version=artifact.code_version)
        for item in reserved_items:
            task_id = str(item.task_id or "").strip()
            record: Optional[TaskState] = None
            try:
                if item.HasField("transport_payload") and str(item.transport_payload.codec or "").strip():
                    item_serialization_mode = str(item.transport_payload.codec or "").strip().lower()
                    item_uses_transport_payload = True
                    decoded_payload = decode_transport_payload_bytes(
                        item.transport_payload.codec,
                        item.transport_payload.version,
                        item.transport_payload.payload,
                        context="taskpool_session",
                    )
                else:
                    item_uses_transport_payload = False
                    raw_payload = struct_to_python(item.payload)
                    item_serialization_mode = detect_transport_mode(raw_payload, default="legacy_v1")
                    decoded_payload = decode_payload_from_transport(
                        raw_payload,
                        policy=get_payload_policy("task_submit"),
                        mode=item_serialization_mode,
                        context="taskpool_session",
                    )
                now = utc_now()
                record = TaskState(
                    task_id=task_id,
                    client_id=pool.pool_id,
                    job_id=normalized_job_id,
                    code_version=pool.code_version,
                    runtime_key=str(item.runtime_key or "").strip(),
                    execution_mode=pb2.EXECUTION_MODE_PERSISTENT,
                    payload=decoded_payload,
                    timeout_hint_sec=max(0, item.timeout_hint_sec),
                    priority=max(1, item.priority or 1),
                    status=pb2.TASK_STATUS_RUNNING,
                    attempt=1,
                    worker_id=f"task-pool:{pool.pool_id}",
                    lease_id=str(uuid.uuid4()),
                    started_at=now,
                    last_heartbeat_at=now,
                    serialization_mode=item_serialization_mode,
                    use_transport_result=item_uses_transport_payload,
                )
                with self._cv:
                    current_pool = self._task_pools.get(normalized_pool_id)
                    if current_pool is None:
                        self._pool_task_reserved_ids.discard(task_id)
                        rejected.append(
                            pb2.TaskRejected(
                                task_id=task_id,
                                code=pb2.ERROR_CODE_INTERNAL_ERROR,
                                message="task pool not found",
                            )
                        )
                        continue
                    if current_pool.status != "RUNNING":
                        self._pool_task_reserved_ids.discard(task_id)
                        rejected.append(
                            pb2.TaskRejected(
                                task_id=task_id,
                                code=pb2.ERROR_CODE_INTERNAL_ERROR,
                                message="task pool not running",
                            )
                        )
                        continue
                    self._pool_task_reserved_ids.discard(task_id)
                    self._pool_tasks[task_id] = record
            except Exception as exc:
                with self._cv:
                    self._pool_task_reserved_ids.discard(task_id)
                    current = self._pool_tasks.get(task_id)
                    if record is not None and current is record:
                        self._pool_tasks.pop(task_id, None)
                rejected.append(
                    pb2.TaskRejected(
                        task_id=task_id,
                        code=pb2.ERROR_CODE_INTERNAL_ERROR,
                        message=f"prepare task failed: {exc}",
                    )
                )
                continue

            try:
                build_start = time.perf_counter()
                execute_spec = _build_execute_spec(
                    artifact,
                    object_dir=self._object_dir,
                    work_dir=work_dir,
                    method_name=artifact.entry_callable,
                    payload=self._resolve_memory_object_refs_in_payload_locked(record.payload),
                    payload_mode="task_submit",
                    serialization_mode=item_serialization_mode,
                    use_transport_result=item_uses_transport_payload,
                    managed_globals_scope_dir=pool.managed_globals_scope_dir,
                    managed_globals_digest=pool.managed_globals_digest,
                )
                record.dispatch_build_execute_spec_ms = (time.perf_counter() - build_start) * 1000.0
                self._executor_host.submit_pool_task(
                    pool_id=pool.pool_id,
                    task_id=task_id,
                    attempt=record.attempt,
                    execute_spec=execute_spec,
                )
            except Exception as exc:
                with self._cv:
                    current = self._pool_tasks.get(task_id)
                    if current is record:
                        self._pool_tasks.pop(task_id, None)
                rejected.append(
                    pb2.TaskRejected(
                        task_id=task_id,
                        code=pb2.ERROR_CODE_INTERNAL_ERROR,
                        message=f"submit task to executor failed: {exc}",
                    )
                )
                continue

            with self._cv:
                current_pool = self._task_pools.get(normalized_pool_id)
                if current_pool is not None:
                    current_pool.task_count += 1
            accepted.append(pb2.TaskAccepted(task_id=task_id, status=pb2.TASK_STATUS_QUEUED))
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
        if results:
            with self._cv:
                for item in results:
                    task_id = str(item.task_id or "").strip()
                    if not task_id:
                        continue
                    task = self._pool_tasks.get(task_id)
                    if task is None:
                        continue
                    if self._pool_task_is_terminal_status(int(task.status or 0)):
                        self._pool_tasks.pop(task_id, None)
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
        stop_executor = None
        with self._lock:
            pool = self._task_pools.get(normalized)
            if pool is None:
                raise KeyError("task pool not found")
            if pool.owner_client_id != str(owner_client_id or "").strip():
                raise PermissionError("owner_client_id mismatch")
            if pool.pool_token != str(pool_token or "").strip():
                raise PermissionError("pool_token mismatch")
            if self._executor_host is not None and pool.executor_ready:
                stop_executor = (self._executor_host, str(pool.pool_id))
            pool.executor_ready = False
            pool.alive_workers = 0
            pool.status = "STOPPED"
            pool.lease_expire_at = utc_now()
            closed_pool = pool

        if stop_executor is not None:
            executor_host, closed_pool_id = stop_executor
            self._submit_stop_task_pool(executor_host, pool_id=closed_pool_id)
        return closed_pool

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
            pool.alive_workers = max(0, int(pool.worker_count or 0))
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
        serialization_mode: str = "",
        use_transport_result: Optional[bool] = None,
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
                payload_mode="http_call",
                serialization_mode=str(serialization_mode or "").strip().lower(),
                use_transport_result=use_transport_result,
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
                    self.data_store.register_stored_result(result)
                result = self.data_store.data_ref_from_stored_artifact(result)
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
            return 200, {"ok": True, "method": requested_method, "data": {} if result is None else result}
        if status_text == "FAILED_USER":
            _ok, normalized_error_type, normalized_error_message = normalize_invoke_error(
                status_text,
                error_type=err_type,
                error_message=err_message,
                user_fallback="user error",
                infra_fallback="infra error",
            )
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
                        error_type=normalized_error_type,
                        error_message=normalized_error_message,
                    )
            return 400, {
                "ok": False,
                "method": requested_method,
                "error_type": normalized_error_type,
                "error": normalized_error_message,
            }
        _ok, normalized_error_type, normalized_error_message = normalize_invoke_error(
            status_text,
            error_type=err_type,
            error_message=err_message,
            user_fallback="user error",
            infra_fallback="infra error",
        )
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
                    error_type=normalized_error_type,
                    error_message=normalized_error_message,
                )
        return 503, {
            "ok": False,
            "method": requested_method,
            "error_type": normalized_error_type,
            "error": normalized_error_message,
        }

    def _invoke_service_http(
        self,
        service_id: str,
        method: str,
        payload: dict,
        service_token: str,
        timeout_sec: float,
        serialization_mode: str = "",
        use_transport_result: bool = False,
    ) -> Tuple[int, Dict[str, object]]:
        return self._invoke_service_call(
            service_id=service_id,
            method=method,
            payload=payload,
            service_token=service_token,
            timeout_sec=timeout_sec,
            serialization_mode=serialization_mode,
            use_transport_result=use_transport_result,
        )

    def _service_extra_get_http(
        self,
        service_id: str,
        path_parts: List[str],
        query: Dict[str, List[str]],
    ) -> Optional[Tuple[object, ...]]:
        del service_id
        del query
        if len(path_parts) != 2 or path_parts[0] != "objects":
            return None
        object_id = unquote(str(path_parts[1] or ""))
        artifact = self.get_object_artifact(object_id)
        if getattr(artifact, "storage_backend", "file") == "segment":
            with open(artifact.segment_path, "rb") as fp:
                fp.seek(max(0, int(getattr(artifact, "segment_offset", 0) or 0)))
                body = fp.read(max(0, int(getattr(artifact, "segment_length", artifact.size_bytes) or artifact.size_bytes)))
        else:
            with open(artifact.path, "rb") as fp:
                body = fp.read()
        return 200, body, "application/octet-stream"

    def call_service(
        self,
        *,
        service_id: str,
        method: str,
        payload: dict,
        service_token: str,
        timeout_sec: float,
        serialization_mode: str = "",
        use_transport_result: Optional[bool] = None,
    ) -> Tuple[int, Dict[str, object]]:
        return self._invoke_service_call(
            service_id=service_id,
            method=method,
            payload=payload,
            service_token=service_token,
            timeout_sec=timeout_sec,
            serialization_mode=serialization_mode,
            use_transport_result=use_transport_result,
        )

    def update_service_globals(
        self,
        *,
        owner_client_id: str,
        service_id: str,
        service_token: str,
        values: Dict[str, Any],
        serialization_mode: str = "",
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
            globals_digest, updated_names = self._update_managed_globals_state(
                state,
                values=values,
                serialization_mode=serialization_mode,
            )
            session.managed_globals_scope_dir = state.scope_dir
            session.managed_globals_digest = globals_digest
            executor_host = self._executor_host
            service_id = session.service_id
            worker_count = session.worker_count
        if artifact is None or executor_host is None:
            return globals_digest, updated_names
        self._execute_warmup(
            executor_host=executor_host,
            scope="service",
            key=service_id,
            worker_count=worker_count,
            execute_spec=_build_execute_spec(
                artifact,
                object_dir=self._object_dir,
                work_dir=_code_data_dir(self._artifact_dir, code_version=artifact.code_version),
                method_name=next(iter(session.methods.keys()), artifact.entry_callable),
                payload={},
                payload_mode="http_call",
                managed_globals_scope_dir=state.scope_dir,
                managed_globals_digest=globals_digest,
                warmup_only=True,
            ),
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
        serialization_mode: str = "",
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
            globals_digest, updated_names = self._update_managed_globals_state(
                state,
                values=values,
                serialization_mode=serialization_mode,
            )
            self._runtime_managed_globals[(normalized_client_id, normalized_code_version, normalized_runtime_key)] = state
            executor_host = self._executor_host
            pool = self._task_pools.get(normalized_client_id)
            if pool is not None:
                pool.managed_globals_scope_dir = state.scope_dir
                pool.managed_globals_digest = globals_digest
            worker_count = int(pool.worker_count if pool is not None else self.worker_capacity)
        if artifact is None or executor_host is None:
            return globals_digest, updated_names
        execute_spec = _build_execute_spec(
            artifact,
            object_dir=self._object_dir,
            work_dir=_code_data_dir(self._artifact_dir, code_version=artifact.code_version),
            method_name=artifact.entry_callable,
            payload={},
            payload_mode="task_submit",
            managed_globals_scope_dir=state.scope_dir,
            managed_globals_digest=globals_digest,
            warmup_only=True,
        )
        warmup_started = time.monotonic()
        if pool is not None:
            self._execute_warmup(
                executor_host=executor_host,
                scope="pool",
                key=pool.pool_id,
                worker_count=worker_count,
                execute_spec=execute_spec,
            )
            with self._lock:
                current_pool = self._task_pools.get(pool.pool_id)
                if current_pool is not None:
                    self._record_task_pool_lifecycle_timing_locked(
                        current_pool,
                        metric="warmup",
                        elapsed_ms=(time.monotonic() - warmup_started) * 1000.0,
                    )
        else:
            self._execute_warmup(
                executor_host=executor_host,
                scope="runtime",
                key=normalized_runtime_key,
                worker_count=worker_count,
                execute_spec=execute_spec,
            )
        return globals_digest, updated_names

    def _service_status_http(self, service_id: str) -> Tuple[int, Dict[str, object]]:
        try:
            info = self.service_status_info(service_id)
        except KeyError:
            return 404, {"ok": False, "error": "service not found"}
        return 200, {"ok": True, "service": info}

    def _service_methods_http(self, service_id: str, include_docs: bool) -> Tuple[int, Dict[str, object]]:
        del include_docs
        try:
            methods = self.list_service_methods(service_id)
        except KeyError:
            return 404, {"ok": False, "error": "service not found"}
        return 200, {"ok": True, "service_id": str(service_id or ""), "methods": methods}

    def service_status_info(self, service_id: str) -> Dict[str, object]:
        with self._lock:
            session = self._services.get(service_id)
            if session is None:
                raise KeyError("service not found")
            return _build_service_status_info(
                session,
                in_flight=self._service_inflight_locked(session),
            )

    def metrics(self) -> Dict[str, int]:
        with self._lock:
            queued = sum(1 for task in self._pool_tasks.values() if task.status == pb2.TASK_STATUS_QUEUED)
            service_inflight = sum(self._service_inflight_locked(session) for session in self._services.values())
            pool_inflight = sum(1 for task in self._pool_tasks.values() if task.status == pb2.TASK_STATUS_RUNNING)
            inflight = service_inflight + pool_inflight
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
                out.append(_build_service_route_report(session, in_flight=self._service_inflight_locked(session)))
            return out

    def service_report_payloads(self, *, include_stopped: bool = False) -> List[Dict[str, object]]:
        with self._lock:
            out: List[Dict[str, object]] = []
            for session in self._services.values():
                if not include_stopped and session.status == pb2.SERVICE_STATUS_STOPPED:
                    continue
                out.append(_build_service_report_payload(session, in_flight=self._service_inflight_locked(session)))
            return out

    def active_runtime_keys(self, *, limit: int = 10) -> List[str]:
        with self._lock:
            stats: Dict[str, Tuple[int, int, float]] = {}
            now_ts = utc_now().timestamp()
            for task in self._pool_tasks.values():
                if task.status not in (pb2.TASK_STATUS_QUEUED, pb2.TASK_STATUS_RUNNING):
                    continue
                runtime_key = str(task.runtime_key or task.client_id or task.code_version).strip() or str(task.code_version or "")
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

    def _drain_executor_events(self) -> None:
        with self._cv:
            self._ensure_executor_host_alive_locked()
            if self._executor_host is None:
                return
            for item in self._executor_host.drain_events():
                kind = str(item.get("kind", "") or "")
                if kind == "pool_executor_rebuilt":
                    pool_id = str(item.get("pool_id", "") or "")
                    pool = self._task_pools.get(pool_id)
                    if pool is not None:
                        self._increment_task_pool_metric_locked(
                            pool,
                            key="executor_rebuild_count",
                            delta=max(1, int(item.get("recoveries", 1) or 1)),
                        )
                    continue
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
                    ok, normalized_error_type, normalized_error_message = normalize_invoke_error(
                        status_text,
                        error_type=err_type,
                        error_message=err_message,
                        user_fallback="user function failed",
                        infra_fallback="infra failure",
                    )
                    if status_text == "FAILED_USER":
                        task.status = pb2.TASK_STATUS_FAILED_USER
                        task.result = None
                        task.error_type = normalized_error_type
                        task.error_message = normalized_error_message
                    elif status_text == "FAILED_INFRA":
                        task.status = pb2.TASK_STATUS_FAILED_INFRA
                        task.result = None
                        task.error_type = normalized_error_type
                        task.error_message = normalized_error_message
                    else:
                        task.status = pb2.TASK_STATUS_SUCCEEDED
                        if isinstance(result, StoredResultArtifact):
                            self.data_store.register_stored_result(result)
                            task.result = self.data_store.data_ref_from_stored_artifact(result)
                        else:
                            task.result = {} if result is None else result
                        task.error_type = ""
                        task.error_message = ""
                    if pool is not None:
                        pool.returned_count += 1
                        self._record_task_pool_timing_locked(
                            pool,
                            method=pool.task_method,
                            ok=ok,
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
    def _dispatch_loop(self) -> None:
        while not self._stop_event.is_set():
            self._drain_executor_events()
            with self._cv:
                self._ensure_executor_host_alive_locked()
            self._drain_executor_events()
            self._stop_event.wait(self.executor_poll_interval_sec)

    def _monitor_loop(self) -> None:
        while not self._stop_event.wait(self.monitor_interval_sec):
            self._handle_service_timeouts()

    def _handle_service_timeouts(self) -> None:
        now = utc_now()
        with self._lock:
            for session in self._services.values():
                if session.status != pb2.SERVICE_STATUS_RUNNING:
                    continue
                if bool(getattr(session, "node_managed", False)):
                    continue
                if now <= session.lease_expire_at:
                    continue
                self._stop_service_locked(session, reason="owner heartbeat timeout")
            for pool in self._task_pools.values():
                if not pool.is_running():
                    continue
                if now <= pool.lease_expire_at:
                    continue
                if self._executor_host is not None and pool.executor_ready:
                    self._submit_stop_task_pool(self._executor_host, pool_id=pool.pool_id)
                pool.executor_ready = False
                pool.alive_workers = 0
                pool.status = "STOPPED"
