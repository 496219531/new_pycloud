from __future__ import annotations

"""Authoritative V1 task-pool implementation."""

import asyncio
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
import contextlib
from dataclasses import dataclass, replace
from datetime import timedelta
import hashlib
import importlib
import inspect
import logging
import math
import os
from pathlib import Path
import sys
import tempfile
import threading
import time
from typing import Any, AsyncIterator, Callable, Dict, Iterable, Iterator, List, Optional, Sequence, Set, Tuple, Union
import uuid

from google.protobuf import struct_pb2

from pycloud_parallel.controlplane.artifact import (
    _default_entry_module_for_module,
    _normalize_artifact_input,
    _prepare_artifact,
    _resolve_package_format,
)
from pycloud_parallel.controlplane.config import OBJECT_CHUNK_SIZE_BYTES, get_payload_policy, get_taskpool_heartbeat_timeout_sec
from pycloud_parallel.controlplane.effective_policy import (
    EffectivePolicy,
    resolve_effective_policy,
    should_use_raw_bytes_payload,
)
from pycloud_parallel.controlplane.infocenter_client import InfoCenterNode, _node_instance_key_from_node
from pycloud_parallel.controlplane.node_control_transport import (
    new_node_control_client as _new_node_control_client,
    node_control_target_for_node as _node_control_target_for_node,
)
from pycloud_parallel.controlplane.policy_profile import (
    get_default_policy_id_for_binding,
    get_policy_profile,
)
from pycloud_parallel.controlplane.data_store import StoredDataArtifact
from pycloud_parallel.controlplane.serialization import LOCAL_IPC_SERIALIZATION_MODE
from pycloud_parallel.controlplane.serialization_mode import resolve_effective_serialization_mode
from pycloud_parallel.controlplane.state_time import dt_to_ts, utc_now
from pycloud_parallel.controlplane.session_model import ExecutionSessionStatus, SessionBinding, SessionIdentity
from pycloud_parallel.controlplane.replica_client import NativeTaskPoolClient
from pycloud_parallel.controlplane.session_handle import ExecutionReplicaHandle
from pycloud_parallel.controlplane.serialization import (
    detect_transport_mode,
    encode_transport_payload_bytes,
    serialize_inline_payload,
    struct_to_python,
)
from pycloud_parallel.controlplane.task_backend import _TaskPoolCallProxy
from pycloud_parallel.execution.base import ExecutionItem, TaskExecutionSession
from pycloud_parallel.execution.failover import (
    CandidateBreakerState,
    REMOTE_INFRA_FAILED,
    SUBMIT_FAILED,
    candidate_allowed,
    mark_candidate_failure,
    mark_candidate_success,
)
from pycloud_parallel.execution.managed_globals import update_managed_globals_across_replicas
from pycloud_parallel.execution.progress import ProgressOption, ProgressReporter, is_progress_option
from pycloud_parallel.execution.deployment_create_helper import (
    dispatch_create_requests,
    normalize_initial_globals,
    prepare_deployment_artifact,
    run_replica_create_recovery_loop,
    should_retry_replica_create_failures,
)
from pycloud_parallel.execution.error_classifier import ErrorCategory, classify_error, is_retryable_compensation_failure
from pycloud_parallel.execution.scheduler import (
    SchedulerCandidate,
    SchedulerState,
    resolve_taskpool_strategy,
    select_one_candidate,
)
from pycloud_parallel.execution.support import (
    _get_local_ip,
    _is_node_identity_mismatch_error,
    _mark_infocenter_node_lost_on_identity_mismatch,
    _prepare_code_blob,
    _prepare_task_payload_for_submit,
    _put_data_via_clients,
    _resolve_public_target_arg,
    _retry_infocenter_request,
    _summarize_discovered_nodes,
)
from pycloud_parallel.data.ref import DataRef, maybe_data_ref, normalize_object_format, object_id_from_sha256_hex
from pycloud_parallel.proto.v1 import pycloud_v1_pb2 as pb2


logger = logging.getLogger(__name__)


def _taskpool_create_rpc_timeout_sec(timeout_sec: float) -> float:
    try:
        overall = float(timeout_sec or 0.0)
    except (TypeError, ValueError):
        overall = 0.0
    if overall <= 0.0:
        return 30.0
    return max(10.0, min(30.0, overall * 3.0))


def _resolve_owner_api_token(api_token: str = "") -> str:
    return str(api_token or os.getenv("PYCLOUD_API_TOKEN", "") or "").strip()
_TASK_POOL_CLOSE_RETRY_DELAYS_SEC = (0.0, 0.5, 1.0, 2.0)
_DEFAULT_MAX_IN_FLIGHT_WORKER_FACTOR = 1.5


def _emit_taskpool_notice(message: str) -> None:
    text = str(message or "").strip()
    if not text:
        return
    print(f"[TaskPool] {text}", file=sys.stderr, flush=True)


def _format_pool_route_summary(routes: Sequence[Dict[str, object]]) -> str:
    rows = []
    for item in routes:
        node_instance_id = str(item.get("node_instance_id", "") or "")
        node_id = str(item.get("node_id", "") or "")
        control_addr = str(item.get("control_addr", "") or "")
        pool_id = str(item.get("pool_id", "") or "")
        pool_name = str(item.get("pool_name", "") or "")
        node_label = node_id or node_instance_id or "-"
        rows.append(
            f"{node_label}/{node_instance_id or '-'}@{control_addr or '-'}"
            f"(pool_id={pool_id or '-'}, pool_name={pool_name or '-'})"
        )
    return "[" + ", ".join(rows) + "]"


class _IndexedPayloadBuffer:
    def __init__(self, payloads: Iterable[Dict[str, object]]) -> None:
        self._payload_iter = iter(payloads)
        self._retry_payloads: "deque[Tuple[int, Dict[str, object]]]" = deque()
        self._input_exhausted = False
        self._next_index = 0

    @property
    def exhausted(self) -> bool:
        return self._input_exhausted

    @property
    def has_retry(self) -> bool:
        return bool(self._retry_payloads)

    @property
    def submitted_count(self) -> int:
        return max(0, int(self._next_index or 0))

    def next(self) -> Optional[Tuple[int, Dict[str, object]]]:
        if self._retry_payloads:
            index, payload = self._retry_payloads.popleft()
            return int(index), payload if isinstance(payload, dict) else {}
        if self._input_exhausted:
            return None
        try:
            raw_payload = next(self._payload_iter)
        except StopIteration:
            self._input_exhausted = True
            return None
        if not isinstance(raw_payload, dict):
            logger.warning("task payload item is not a mapping; wrapping it as {'value': item}")
            raw_payload = {"value": raw_payload}
        index = self._next_index
        self._next_index += 1
        return index, raw_payload

    def requeue_front(self, items: Sequence[Tuple[int, Dict[str, object], Any]]) -> None:
        for index, payload, _item in reversed(list(items)):
            self._retry_payloads.appendleft((int(index), payload if isinstance(payload, dict) else {}))


class _SizedPayloadIterable:
    def __init__(self, payloads: Iterable[Dict[str, object]], total: int) -> None:
        self._payloads = payloads
        self._total = max(0, int(total or 0))

    def __iter__(self) -> Iterator[Dict[str, object]]:
        return iter(self._payloads)

    def __len__(self) -> int:
        return self._total


@dataclass
class _TaskReplayRecord:
    logical_index: int
    logical_key: object
    original_payload: Dict[str, object]
    current_task_id: str
    current_node_id: str
    attempt: int
    submitted_at: float
    last_error: str = ""
    timeout_retry_count: int = 0


def _infocenter_client(*args, **kwargs):
    from pycloud_parallel.controlplane.infocenter_client import InfoCenterClient

    return InfoCenterClient(*args, **kwargs)


def _task_submit_item_http_payload_size(item: pb2.TaskSubmitItem) -> int:
    if item.HasField("transport_payload") and str(item.transport_payload.codec or "").strip():
        return len(bytes(item.transport_payload.payload or b"")) + 512
    try:
        return int(item.ByteSize()) + 512
    except Exception:
        return max(1024, len(str(item).encode("utf-8", errors="ignore")))


def _node_submit_http_body_limit(node: object, pool: object) -> int:
    values: List[int] = []
    for source in (getattr(node, "capability", None), getattr(pool, "capability", None)):
        for key in ("max_http_body_bytes", "max_control_send_bytes", "max_control_recv_bytes"):
            if isinstance(source, dict):
                value = int(source.get(key, 0) or 0)
            else:
                value = int(getattr(source, key, 0) or 0)
            if value > 0:
                values.append(value)
    if not values:
        return 0
    return max(1024, min(values))


def _local_direct_module_name(source: Any, entry_module: Any = "") -> str:
    explicit = str(entry_module or "").strip()
    if explicit:
        return explicit
    if inspect.ismodule(source):
        return _default_entry_module_for_module(source)
    if callable(source):
        return str(getattr(source, "__module__", "") or "").strip()
    if isinstance(source, str) and source.replace("_", "").replace(".", "").isalnum():
        return str(source or "").strip()
    return ""


def _local_direct_callable_name(source: Any, entry_callable: Any = "run") -> str:
    explicit = str(entry_callable or "").strip()
    if explicit and explicit != "run":
        return explicit
    if callable(source) and not inspect.ismodule(source):
        return str(getattr(source, "__name__", "") or "").strip() or explicit or "run"
    return explicit or "run"


def _taskpool_local_uses_direct_callable(
    *,
    source: Any,
    artifact: Optional[Any],
    deps: Optional[Any],
    package_format: str,
    resource_paths: Optional[Sequence[Any]],
) -> bool:
    if artifact is not None:
        return False
    if deps is not None:
        return False
    if str(package_format or "").strip():
        return False
    if any(str(item or "").strip() for item in list(resource_paths or ())):
        return False
    if inspect.ismodule(source) or callable(source):
        return True
    return bool(_local_direct_module_name(source))


class _LocalTaskPoolNodeClient:
    def __init__(self, state, *, pool: Any = None) -> None:
        self._state = state
        self.target = ""
        self.node_id = str(getattr(state, "node_id", "") or "")
        self.node_instance_id = str(getattr(state, "node_instance_id", "") or "")
        self.owner_client_id = str(getattr(pool, "owner_client_id", "") or "")
        self.pool_id = str(getattr(pool, "pool_id", "") or "")
        self.pool_token = str(getattr(pool, "pool_token", "") or "")
        self.code_version = str(getattr(pool, "code_version", "") or "")
        self.worker_count = max(1, int(getattr(pool, "worker_count", 1) or 1))
        self.heartbeat_timeout_sec = max(1, int(getattr(pool, "heartbeat_timeout_sec", 30) or 30))
        self.pool_name = str(getattr(pool, "pool_name", "") or self.pool_id or "local-task-pool")
        self.idle_ttl_sec = max(0, int(getattr(pool, "idle_ttl_sec", 0) or 0))
        self.status = str(getattr(pool, "status", "") or "RUNNING")
        self.created_at = getattr(pool, "created_at", utc_now())
        self.last_heartbeat_at = getattr(pool, "last_heartbeat_at", self.created_at)
        self.lease_expire_at = getattr(
            pool,
            "lease_expire_at",
            self.last_heartbeat_at + timedelta(seconds=self.heartbeat_timeout_sec),
        )

    def close(self) -> None:
        self._state.close()

    def upload_object_from_bytes(
        self,
        *,
        blob: bytes,
        format: str = "",
        chunk_size: int = OBJECT_CHUNK_SIZE_BYTES,
        **kwargs: Any,
    ) -> DataRef:
        del chunk_size, kwargs
        data = bytes(blob or b"")
        digest = hashlib.sha256(data).hexdigest()
        object_id = object_id_from_sha256_hex(digest)
        fmt = normalize_object_format(format, default="bin")
        tmp = tempfile.NamedTemporaryFile(
            mode="wb",
            prefix="pycloud-local-object-",
            suffix=f".{fmt}",
            delete=False,
            dir=str(self._state.artifact_dir),
        )
        try:
            with tmp:
                tmp.write(data)
            artifact, _cached = self._state.put_object_from_uploaded_file(
                object_id=object_id,
                format=fmt,
                uploaded_path=tmp.name,
                actual_sha256=digest,
                size_bytes=len(data),
            )
        except Exception:
            Path(tmp.name).unlink(missing_ok=True)
            raise
        return DataRef(
            ref_id=artifact.object_id,
            storage_id=artifact.object_id,
            format=artifact.format,
            size_bytes=artifact.size_bytes,
            materialize_as="path",
            locator_kind="node_local",
            node_id=self.node_id,
            node_instance_id=self.node_instance_id,
        )

    def upload_object_from_file(
        self,
        *,
        file_path: str,
        format: str = "",
        chunk_size: int = OBJECT_CHUNK_SIZE_BYTES,
        **kwargs: Any,
    ) -> DataRef:
        del kwargs
        path = Path(file_path)
        fmt = normalize_object_format(format, source_name=path.name, default="bin")
        effective_chunk_size = max(1, int(chunk_size or OBJECT_CHUNK_SIZE_BYTES))
        hasher = hashlib.sha256()
        size_bytes = 0
        tmp = tempfile.NamedTemporaryFile(
            mode="wb",
            prefix="pycloud-local-object-",
            suffix=f".{fmt}",
            delete=False,
            dir=str(self._state.artifact_dir),
        )
        try:
            with tmp:
                with path.open("rb") as fp:
                    while True:
                        chunk = fp.read(effective_chunk_size)
                        if not chunk:
                            break
                        hasher.update(chunk)
                        size_bytes += len(chunk)
                        tmp.write(chunk)
            digest = hasher.hexdigest()
            object_id = object_id_from_sha256_hex(digest)
            artifact, _cached = self._state.put_object_from_uploaded_file(
                object_id=object_id,
                format=fmt,
                uploaded_path=tmp.name,
                actual_sha256=digest,
                size_bytes=size_bytes,
            )
        except Exception:
            Path(tmp.name).unlink(missing_ok=True)
            raise
        return DataRef(
            ref_id=artifact.object_id,
            storage_id=artifact.object_id,
            format=artifact.format,
            size_bytes=artifact.size_bytes,
            materialize_as="path",
            locator_kind="node_local",
            node_id=self.node_id,
            node_instance_id=self.node_instance_id,
        )

    def submit_pool_tasks(
        self,
        *,
        pool_id: str,
        pool_token: str,
        tasks: Sequence[pb2.TaskSubmitItem],
        job_id: str = "",
    ) -> pb2.SubmitTasksResponse:
        accepted, rejected = self._state.submit_pool_tasks(
            pool_id=pool_id,
            pool_token=pool_token,
            tasks=list(tasks),
            job_id=job_id,
        )
        return pb2.SubmitTasksResponse(ok=True, accepted=accepted, rejected=rejected, node_credit=0)

    def pull_pool_results(
        self,
        *,
        pool_id: str,
        pool_token: str,
        limit: int = 100,
        wait_ms: int = 0,
        cursor: str = "",
    ) -> pb2.PullResultsResponse:
        results, next_cursor = self._state.pull_pool_results(
            pool_id=pool_id,
            pool_token=pool_token,
            limit=limit,
            wait_ms=wait_ms,
            cursor=cursor,
        )
        return pb2.PullResultsResponse(ok=True, results=results, next_cursor=next_cursor)

    def close_task_pool(
        self,
        *,
        owner_client_id: str,
        pool_id: str,
        pool_token: str,
        reason: str = "",
    ) -> pb2.CloseTaskPoolResponse:
        self._state.close_task_pool(
            owner_client_id=owner_client_id,
            pool_id=pool_id,
            pool_token=pool_token,
            reason=reason,
        )
        return pb2.CloseTaskPoolResponse(ok=True, accepted=True)

    def heartbeat_task_pool(
        self,
        *,
        owner_client_id: str,
        pool_id: str,
        pool_token: str,
        seq: int = 0,
        timeout_sec: Optional[float] = None,
    ) -> pb2.HeartbeatTaskPoolResponse:
        del seq, timeout_sec
        pool = self._state.heartbeat_task_pool(
            owner_client_id=owner_client_id,
            pool_id=pool_id,
            pool_token=pool_token,
        )
        return pb2.HeartbeatTaskPoolResponse(
            ok=True,
            accepted=True,
            next_heartbeat_in_sec=max(1, int(pool.heartbeat_timeout_sec or 30) // 2),
        )

    def cancel_pool_job(
        self,
        *,
        pool_id: str,
        pool_token: str,
        job_id: str,
        reason: str = "",
    ) -> pb2.CancelJobResponse:
        queued, running, done, missing = self._state.cancel_pool_job(
            pool_id=pool_id,
            pool_token=pool_token,
            job_id=job_id,
            reason=reason,
        )
        return pb2.CancelJobResponse(
            ok=True,
            queued_cancelled=queued,
            running_marked=running,
            already_done=done,
            not_found=missing,
        )

    def get_task_pool_status(self, *, pool_id: str, pool_token: str) -> pb2.TaskPoolStatusInfo:
        from pycloud_parallel.controlplane.state_time import dt_to_ts

        pool = self._state.task_pool(pool_id)
        self._state._require_pool_token(pool, pool_token)  # noqa: SLF001
        info = self._state.task_pool_status_info(pool_id)
        return pb2.TaskPoolStatusInfo(
            pool_id=str(info.get("pool_id", "")),
            owner_client_id=str(info.get("owner_client_id", "")),
            pool_name=str(info.get("pool_name", "")),
            code_version=str(info.get("code_version", "")),
            worker_count=int(info.get("worker_count", 0) or 0),
            heartbeat_timeout_sec=int(info.get("heartbeat_timeout_sec", 0) or 0),
            status=str(info.get("status", "")),
            task_count=int(info.get("task_count", 0) or 0),
            created_at=dt_to_ts(info["created_at"]),
            last_heartbeat_at=dt_to_ts(info["last_heartbeat_at"]),
            lease_expire_at=dt_to_ts(info["lease_expire_at"]),
        )

    def update_runtime_globals_prepared(
        self,
        *,
        client_id: str,
        code_version: str,
        runtime_key: str,
        code_token: str,
        prepared_values: Dict[str, object],
        serialization_mode: str = "",
        effective_policy: Optional[EffectivePolicy] = None,
    ) -> pb2.UpdateRuntimeGlobalsResponse:
        del effective_policy
        digest, updated = self._state.update_runtime_globals(
            client_id=client_id,
            code_version=code_version,
            runtime_key=runtime_key,
            code_token=code_token,
            values=dict(prepared_values or {}),
            serialization_mode=serialization_mode,
        )
        return pb2.UpdateRuntimeGlobalsResponse(
            ok=True,
            code_version=code_version,
            runtime_key=runtime_key or code_version,
            globals_digest=digest,
            updated_names=updated,
        )

    def fetch_result_data(self, task_result: pb2.TaskResult, *, target_path: str = ""):
        if target_path:
            raise ValueError("local task pool fetch_result_data does not support target_path")
        if task_result.HasField("transport_result") and str(task_result.transport_result.codec or "").strip():
            from pycloud_parallel.controlplane.serialization import decode_transport_payload_bytes

            data = decode_transport_payload_bytes(
                str(task_result.transport_result.codec or ""),
                int(task_result.transport_result.version or 0),
                task_result.transport_result.payload,
                context="taskpool_session",
            )
        else:
            from pycloud_parallel.controlplane.payload_transport import decode_result_from_transport
            from pycloud_parallel.controlplane.serialization import detect_transport_mode, struct_to_python

            raw = struct_to_python(task_result.result)
            data = decode_result_from_transport(
                raw,
                mode=detect_transport_mode(raw, default="legacy_v1"),
                context="taskpool_session",
            )
        ref = maybe_data_ref(data)
        if ref is None:
            return data
        return self._state.data_store.resolve_data_ref(ref)


class _DirectLocalTaskPoolNodeClient(_LocalTaskPoolNodeClient):
    def __init__(
        self,
        *,
        node_id: str,
        node_instance_id: str,
        pool_name: str,
        owner_client_id: str,
        worker_count: int,
        heartbeat_timeout_sec: int,
        idle_ttl_sec: int,
        fn: Callable[..., Any],
        managed_global_names: Sequence[str] = (),
        initial_globals: Optional[Dict[str, object]] = None,
    ) -> None:
        from pycloud_parallel.controlplane.node.results import _data_store_for_object_dir

        self.target = ""
        self.node_id = str(node_id or "")
        self.node_instance_id = str(node_instance_id or self.node_id or "")
        self.owner_client_id = str(owner_client_id or "")
        self.pool_id = f"{pool_name or 'local-task-pool'}-{uuid.uuid4().hex[:10]}"
        self.pool_name = str(pool_name or self.pool_id)
        self.pool_token = uuid.uuid4().hex
        self.code_version = f"direct:{str(getattr(fn, '__module__', '') or '')}.{str(getattr(fn, '__name__', '') or 'run')}"
        self.worker_count = max(1, int(worker_count or 1))
        self.heartbeat_timeout_sec = max(5, int(heartbeat_timeout_sec or 30))
        self.idle_ttl_sec = max(0, int(idle_ttl_sec or 0))
        self.status = "RUNNING"
        self.created_at = utc_now()
        self.last_heartbeat_at = self.created_at
        self.lease_expire_at = self.last_heartbeat_at + timedelta(seconds=self.heartbeat_timeout_sec)
        self._fn = fn
        self._module = inspect.getmodule(fn)
        self._managed_global_names = tuple(str(name).strip() for name in (managed_global_names or ()) if str(name).strip())
        self._executor = ThreadPoolExecutor(max_workers=self.worker_count, thread_name_prefix=f"{self.pool_name}-local")
        self._lock = threading.Condition()
        self._results: "deque[pb2.TaskResult]" = deque()
        self._futures = {}
        self._direct_payloads: Dict[str, object] = {}
        self._direct_results: Dict[str, object] = {}
        self._closed = False
        self._object_root = Path(tempfile.mkdtemp(prefix="pycloud-local-taskpool-"))
        self.artifact_dir = self._object_root
        self.data_store = _data_store_for_object_dir(
            str(self._object_root),
            node_id=self.node_id,
            node_instance_id=self.node_instance_id,
            control_addr="local",
        )
        if initial_globals:
            self._apply_globals(dict(initial_globals or {}))

    def put_direct_payload(self, task_id: str, payload: object) -> None:
        normalized = str(task_id or "").strip()
        if not normalized:
            return
        with self._lock:
            self._direct_payloads[normalized] = payload

    @property
    def _state(self):
        return self

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            futures = list(self._futures.values())
            self._lock.notify_all()
        for future in futures:
            future.cancel()
        self._executor.shutdown(wait=False, cancel_futures=True)

    def upload_object_from_bytes(
        self,
        *,
        blob: bytes,
        format: str = "",
        chunk_size: int = OBJECT_CHUNK_SIZE_BYTES,
        **kwargs: Any,
    ) -> DataRef:
        del chunk_size, kwargs
        data = bytes(blob or b"")
        digest = hashlib.sha256(data).hexdigest()
        object_id = object_id_from_sha256_hex(digest)
        fmt = normalize_object_format(format, default="bin")
        tmp = tempfile.NamedTemporaryFile(
            mode="wb",
            prefix="pycloud-local-object-",
            suffix=f".{fmt}",
            delete=False,
            dir=str(self._object_root),
        )
        try:
            with tmp:
                tmp.write(data)
            artifact = self.data_store.store_path(Path(tmp.name))
        except Exception:
            Path(tmp.name).unlink(missing_ok=True)
            raise
        Path(tmp.name).unlink(missing_ok=True)
        return self.data_store.data_ref_from_stored_artifact(artifact)

    def upload_object_from_file(
        self,
        *,
        file_path: str,
        format: str = "",
        chunk_size: int = OBJECT_CHUNK_SIZE_BYTES,
        **kwargs: Any,
    ) -> DataRef:
        del kwargs
        path = Path(file_path)
        fmt = normalize_object_format(format, source_name=path.name, default="bin")
        effective_chunk_size = max(1, int(chunk_size or OBJECT_CHUNK_SIZE_BYTES))
        tmp = tempfile.NamedTemporaryFile(
            mode="wb",
            prefix="pycloud-local-object-",
            suffix=f".{fmt}",
            delete=False,
            dir=str(self._object_root),
        )
        try:
            with tmp:
                with path.open("rb") as fp:
                    while True:
                        chunk = fp.read(effective_chunk_size)
                        if not chunk:
                            break
                        tmp.write(chunk)
            artifact = self.data_store.store_path(Path(tmp.name))
        except Exception:
            Path(tmp.name).unlink(missing_ok=True)
            raise
        Path(tmp.name).unlink(missing_ok=True)
        return self.data_store.data_ref_from_stored_artifact(artifact)

    def _apply_globals(self, values: Dict[str, object]) -> str:
        if self._module is None:
            if values:
                raise ValueError("direct local task callable has no module globals")
            return ""
        allowed = set(self._managed_global_names)
        if allowed:
            unknown = sorted(str(name) for name in values if str(name) not in allowed)
            if unknown:
                raise ValueError(f"managed globals not declared for local task pool: {unknown}")
        apply_hook = getattr(self._module, "apply_managed_globals", None)
        if callable(apply_hook):
            apply_hook(dict(values or {}))
        else:
            for name, value in dict(values or {}).items():
                setattr(self._module, str(name), value)
        digest_payload = repr(sorted((str(name), repr(value)) for name, value in dict(values or {}).items()))
        return "sha256:" + hashlib.sha256(digest_payload.encode("utf-8")).hexdigest()

    def submit_pool_tasks(
        self,
        *,
        pool_id: str,
        pool_token: str,
        tasks: Sequence[pb2.TaskSubmitItem],
        job_id: str = "",
    ) -> pb2.SubmitTasksResponse:
        self._require_pool(pool_id=pool_id, pool_token=pool_token)
        accepted: List[pb2.TaskAccepted] = []
        rejected: List[pb2.TaskRejected] = []
        with self._lock:
            if self._closed or self.status != "RUNNING":
                raise RuntimeError("task pool not running")
            existing = set(self._futures.keys()) | {str(item.task_id or "") for item in self._results}
            for item in tasks:
                task_id = str(item.task_id or "").strip()
                if not task_id:
                    rejected.append(
                        pb2.TaskRejected(
                            task_id=task_id,
                            code=pb2.ERROR_CODE_INVALID_REQUEST,
                            message="task_id is required",
                        )
                    )
                    continue
                if task_id in existing:
                    rejected.append(
                        pb2.TaskRejected(
                            task_id=task_id,
                            code=pb2.ERROR_CODE_DUPLICATE_TASK,
                            message="duplicate task_id",
                        )
                    )
                    continue
                existing.add(task_id)
                future = self._executor.submit(self._execute_task_item, item, str(job_id or ""))
                self._futures[task_id] = future
                future.add_done_callback(lambda done, current_task_id=task_id: self._complete_future(current_task_id, done))
                accepted.append(pb2.TaskAccepted(task_id=task_id, status=pb2.TASK_STATUS_QUEUED))
        return pb2.SubmitTasksResponse(ok=True, accepted=accepted, rejected=rejected, node_credit=0)

    def _execute_task_item(self, item: pb2.TaskSubmitItem, job_id: str) -> pb2.TaskResult:
        from pycloud_parallel.controlplane.node.execution import _invoke_local_user_callable
        from pycloud_parallel.controlplane.node.results import _normalize_user_return, _resolve_object_refs_in_payload
        from pycloud_parallel.controlplane.payload_transport import decode_payload_from_transport

        task_id = str(item.task_id or "").strip()
        started_at = utc_now()
        item_uses_transport_payload = False
        item_uses_direct_payload = False
        try:
            with self._lock:
                has_direct_payload = task_id in self._direct_payloads
                direct_payload = self._direct_payloads.pop(task_id, None) if has_direct_payload else None
            if has_direct_payload:
                item_uses_direct_payload = True
                item_serialization_mode = LOCAL_IPC_SERIALIZATION_MODE
                payload = direct_payload
            elif item.HasField("transport_payload") and str(item.transport_payload.codec or "").strip():
                from pycloud_parallel.controlplane.serialization import decode_transport_payload_bytes

                item_uses_transport_payload = True
                item_serialization_mode = str(item.transport_payload.codec or "").strip().lower()
                payload = decode_transport_payload_bytes(
                    item.transport_payload.codec,
                    item.transport_payload.version,
                    item.transport_payload.payload,
                    context="taskpool_session",
                )
            else:
                raw_payload = struct_to_python(item.payload)
                item_serialization_mode = detect_transport_mode(raw_payload, default="legacy_v1")
                payload = decode_payload_from_transport(
                    raw_payload,
                    policy=get_payload_policy("task_submit"),
                    mode=item_serialization_mode,
                    context="taskpool_session",
                )
            resolved_payload = _resolve_object_refs_in_payload(payload or {}, object_dir=str(self._object_root))
            ret = _invoke_local_user_callable(self._fn, resolved_payload if isinstance(resolved_payload, dict) else {"value": resolved_payload})
            status_text, result, err_type, err_message = _normalize_user_return(
                ret,
                object_dir=str(self._object_root),
                serialization_mode=item_serialization_mode,
                use_transport_result=item_uses_transport_payload,
            )
            status = pb2.TASK_STATUS_SUCCEEDED
            if status_text == "FAILED_USER":
                status = pb2.TASK_STATUS_FAILED_USER
            elif status_text == "FAILED_INFRA":
                status = pb2.TASK_STATUS_FAILED_INFRA
            if isinstance(result, StoredDataArtifact):
                result = self.data_store.result_ref_from_stored_artifact(result)
            result_kwargs = {
                "task_id": task_id,
                "job_id": job_id,
                "status": status,
                "attempt": 1,
                "started_at": dt_to_ts(started_at),
                "finished_at": dt_to_ts(utc_now()),
                "error": pb2.TaskError(type=err_type, message=err_message),
            }
            if item_uses_direct_payload:
                with self._lock:
                    self._direct_results[task_id] = {} if result is None else result
                result_kwargs["result"] = struct_pb2.Struct()
            elif item_uses_transport_payload:
                result_kwargs["transport_result"] = encode_transport_payload_bytes(
                    {} if result is None else result,
                    mode=item_serialization_mode,
                    context="task result",
                )
            else:
                result_kwargs["result"] = serialize_inline_payload(
                    {} if result is None else result,
                    context="task result",
                    mode=item_serialization_mode,
                )[1]
            return pb2.TaskResult(**result_kwargs)
        except Exception as exc:
            return pb2.TaskResult(
                task_id=task_id,
                job_id=job_id,
                status=pb2.TASK_STATUS_FAILED_USER,
                attempt=1,
                started_at=dt_to_ts(started_at),
                finished_at=dt_to_ts(utc_now()),
                error=pb2.TaskError(type=type(exc).__name__, message=str(exc)),
            )

    def _complete_future(self, task_id: str, future) -> None:
        try:
            result = future.result()
        except Exception as exc:
            result = pb2.TaskResult(
                task_id=str(task_id or ""),
                status=pb2.TASK_STATUS_FAILED_INFRA,
                attempt=1,
                started_at=dt_to_ts(utc_now()),
                finished_at=dt_to_ts(utc_now()),
                error=pb2.TaskError(type=type(exc).__name__, message=str(exc)),
            )
        with self._lock:
            self._futures.pop(str(task_id or ""), None)
            self._results.append(result)
            self._lock.notify_all()

    def pull_pool_results(
        self,
        *,
        pool_id: str,
        pool_token: str,
        limit: int = 100,
        wait_ms: int = 0,
        cursor: str = "",
    ) -> pb2.PullResultsResponse:
        del cursor
        self._require_pool(pool_id=pool_id, pool_token=pool_token)
        deadline = time.monotonic() + max(0.0, float(wait_ms or 0) / 1000.0)
        with self._lock:
            while not self._results and not self._closed and time.monotonic() < deadline:
                self._lock.wait(timeout=max(0.0, deadline - time.monotonic()))
            out: List[pb2.TaskResult] = []
            for _ in range(max(1, int(limit or 100))):
                if not self._results:
                    break
                out.append(self._results.popleft())
        return pb2.PullResultsResponse(ok=True, results=out, next_cursor="")

    def close_task_pool(
        self,
        *,
        owner_client_id: str,
        pool_id: str,
        pool_token: str,
        reason: str = "",
    ) -> pb2.CloseTaskPoolResponse:
        del owner_client_id, reason
        self._require_pool(pool_id=pool_id, pool_token=pool_token)
        self.status = "CLOSED"
        self.close()
        return pb2.CloseTaskPoolResponse(ok=True, accepted=True)

    def heartbeat_task_pool(
        self,
        *,
        owner_client_id: str,
        pool_id: str,
        pool_token: str,
        seq: int = 0,
        timeout_sec: Optional[float] = None,
    ) -> pb2.HeartbeatTaskPoolResponse:
        del owner_client_id, seq, timeout_sec
        self._require_pool(pool_id=pool_id, pool_token=pool_token)
        self.last_heartbeat_at = utc_now()
        self.lease_expire_at = self.last_heartbeat_at + timedelta(seconds=self.heartbeat_timeout_sec)
        return pb2.HeartbeatTaskPoolResponse(ok=True, accepted=True, next_heartbeat_in_sec=max(1, self.heartbeat_timeout_sec // 2))

    def cancel_pool_job(
        self,
        *,
        pool_id: str,
        pool_token: str,
        job_id: str,
        reason: str = "",
    ) -> pb2.CancelJobResponse:
        del job_id, reason
        self._require_pool(pool_id=pool_id, pool_token=pool_token)
        return pb2.CancelJobResponse(ok=True)

    def get_task_pool_status(self, *, pool_id: str, pool_token: str) -> pb2.TaskPoolStatusInfo:
        self._require_pool(pool_id=pool_id, pool_token=pool_token)
        with self._lock:
            task_count = len(self._futures) + len(self._results)
        return pb2.TaskPoolStatusInfo(
            pool_id=self.pool_id,
            owner_client_id=self.owner_client_id,
            pool_name=self.pool_name,
            code_version=self.code_version,
            worker_count=self.worker_count,
            heartbeat_timeout_sec=self.heartbeat_timeout_sec,
            status="RUNNING" if not self._closed else "CLOSED",
            task_count=task_count,
            created_at=dt_to_ts(self.created_at),
            last_heartbeat_at=dt_to_ts(self.last_heartbeat_at),
            lease_expire_at=dt_to_ts(self.lease_expire_at),
        )

    def update_runtime_globals_prepared(
        self,
        *,
        client_id: str,
        code_version: str,
        runtime_key: str,
        code_token: str,
        prepared_values: Dict[str, object],
        serialization_mode: str = "",
        effective_policy: Optional[EffectivePolicy] = None,
    ) -> pb2.UpdateRuntimeGlobalsResponse:
        del client_id, code_version, runtime_key, code_token, serialization_mode, effective_policy
        digest = self._apply_globals(dict(prepared_values or {}))
        return pb2.UpdateRuntimeGlobalsResponse(
            ok=True,
            code_version=self.code_version,
            runtime_key=self.pool_id,
            globals_digest=digest,
            updated_names=sorted(str(key) for key in dict(prepared_values or {}).keys()),
        )

    def _require_pool(self, *, pool_id: str, pool_token: str) -> None:
        if str(pool_id or "") != self.pool_id:
            raise KeyError("task pool not found")
        if str(pool_token or "") != self.pool_token:
            raise RuntimeError("invalid task pool token")

    def fetch_result_data(self, task_result: pb2.TaskResult, *, target_path: str = ""):
        task_id = str(task_result.task_id or "").strip()
        with self._lock:
            if task_id in self._direct_results:
                return self._direct_results.pop(task_id)
        if target_path:
            raise ValueError("local task pool fetch_result_data does not support target_path")
        if task_result.HasField("transport_result") and str(task_result.transport_result.codec or "").strip():
            from pycloud_parallel.controlplane.serialization import decode_transport_payload_bytes

            data = decode_transport_payload_bytes(
                str(task_result.transport_result.codec or ""),
                int(task_result.transport_result.version or 0),
                task_result.transport_result.payload,
                context="taskpool_session",
            )
        else:
            from pycloud_parallel.controlplane.payload_transport import decode_result_from_transport

            raw = struct_to_python(task_result.result)
            data = decode_result_from_transport(
                raw,
                mode=detect_transport_mode(raw, default="legacy_v1"),
                context="taskpool_session",
            )
        ref = maybe_data_ref(data)
        if ref is None:
            return data
        return self.data_store.resolve_data_ref(ref)


def _close_task_pool_replica(pool: Any, *, reason: str) -> None:
    last_exc: Optional[Exception] = None
    for delay_sec in _TASK_POOL_CLOSE_RETRY_DELAYS_SEC:
        if delay_sec > 0.0:
            time.sleep(delay_sec)
        try:
            pool.close(reason=reason)
            last_exc = None
            break
        except Exception as exc:
            last_exc = exc
    if last_exc is not None:
        logger.warning(
            "task pool replica close failed after retries pool_id=%s node_id=%s err=%r",
            getattr(pool, "pool_id", ""),
            getattr(pool, "node_id", ""),
            last_exc,
        )


async def _aiter_from_sync_iterator(iterator) -> AsyncIterator[Any]:
    loop = asyncio.get_running_loop()
    sentinel = object()

    def _next_item():
        try:
            return next(iterator)
        except StopIteration:
            return sentinel

    while True:
        item = await loop.run_in_executor(None, _next_item)
        if item is sentinel:
            return
        yield item


class _TaskPoolSessionBase(TaskExecutionSession):
    """Internal task-pool execution session backed by NodeControl task-pool RPCs."""

    def __init__(
        self,
        *,
        pools: Dict[str, NativeTaskPoolClient],
        nodes: Dict[str, InfoCenterNode],
        task_method: str,
        job_id: str = "",
        serialization_mode: str = "",
        policy_id: str = "",
        effective_policy: Optional[EffectivePolicy] = None,
    ) -> None:
        self._pools = pools
        self.nodes = nodes
        self._task_method = str(task_method or "run").strip() or "run"
        self._job_id = str(job_id or f"pool-{uuid.uuid4().hex[:12]}").strip()
        self._policy_id = (
            str(policy_id or "").strip().lower()
            or get_default_policy_id_for_binding("taskpool_default")
        )
        self.effective_policy = effective_policy or resolve_effective_policy(
            get_policy_profile(self._policy_id),
            requested_mode=serialization_mode,
            context="taskpool_session",
        )
        self._serialization_mode = self.effective_policy.resolved_mode
        self._closed = False
        self._submit_seq = 0
        self._submit_lock = threading.Lock()
        self._pool_cycle = 0
        self._pool_lock = threading.Lock()
        self.failed = False
        self.failures: Dict[str, str] = {}
        self.globals_digests: Dict[str, str] = {}
        self._active_nodes: set[str] = set(self._pools.keys())
        self._pending_task_ids: set[str] = set()
        self._pending_task_node_ids: Dict[str, str] = {}
        self._replay_records: Dict[str, _TaskReplayRecord] = {}
        self._replay_node_index: Dict[str, Set[str]] = {}
        self._scheduler_state = SchedulerState()
        self._submit_breaker_states: Dict[str, CandidateBreakerState] = {
            str(node_id): CandidateBreakerState() for node_id in self._pools.keys()
        }
        self._result_state_lock = threading.Lock()
        self._buffered_result_items: "deque[Tuple[str, pb2.TaskResult]]" = deque()
        self._exclusive_lock = threading.Lock()
        self._exclusive_mode = ""
        self._exclusive_owner_thread_id = 0
        self._exclusive_depth = 0
        self._compensation_spec: Optional[Dict[str, Any]] = None
        self._compensation_lock = threading.Lock()
        self._last_compensation_attempt_at = 0.0
        self._last_managed_globals: Optional[Dict[str, object]] = None
        self.task_retry_count = 0
        self.task_retry_success_count = 0
        self.task_retry_exhausted_count = 0
        self.node_lost_replayed_tasks = 0
        self.node_lost_failed_tasks = 0
        self.retry_prepare_payload_ms = 0.0
        self.retry_submit_ms = 0.0
        self._init_execution_session_state()

    def _is_local_session(self) -> bool:
        if self._pools and all(isinstance(getattr(pool, "_client", None), _LocalTaskPoolNodeClient) for pool in self._pools.values()):
            return True
        return bool(self.nodes) and all(
            str(getattr(node, "control_addr", "") or "").strip().lower() == "local"
            for node in self.nodes.values()
        )

    def _replica_handles(self) -> Dict[str, ExecutionReplicaHandle]:
        return self._pools

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

    def route_summary(self) -> List[Dict[str, object]]:
        routes: List[Dict[str, object]] = []
        for node_key, pool in sorted(self._pools.items()):
            node = self.nodes.get(node_key)
            routes.append(
                {
                    "node_instance_id": str(node_key or ""),
                    "node_id": str(node.node_id if node is not None else getattr(pool, "node_id", "") or ""),
                    "control_addr": str(node.control_addr if node is not None else ""),
                    "pool_id": str(getattr(pool, "pool_id", "") or ""),
                    "pool_name": str(getattr(pool, "pool_name", "") or ""),
                    "owner_client_id": str(getattr(pool, "owner_client_id", "") or ""),
                }
            )
        return routes

    def routes(self) -> List[Dict[str, object]]:
        return self.route_summary()

    def _configure_dynamic_compensation(self, spec: Dict[str, Any]) -> None:
        desired = max(0, int(spec.get("node_count", 0) or 0))
        if desired <= 0:
            self._compensation_spec = None
            return
        self._compensation_spec = dict(spec)

    @staticmethod
    def _is_retryable_compensation_failure(message: str) -> bool:
        return is_retryable_compensation_failure(message, resource_kind="task_pool")

    def _after_keepalive_tick(self) -> None:
        self._maybe_submit_compensation_after_tick(
            self._compensation_spec,
            resource_name=str(getattr(self, "pool_name", "") or getattr(self, "job_id", "") or "")
        )

    def try_compensate_replicas(self) -> int:
        spec = self._compensation_spec
        if not spec or self._closed:
            return 0
        if not self._compensation_lock.acquire(blocking=False):
            return 0
        try:
            desired = max(0, int(spec.get("node_count", 0) or 0))
            active = self._active_replica_snapshot()
            recovery_states = self._build_replica_recovery_states(
                is_retryable_failure=self._is_retryable_compensation_failure,
            )
            failed = {node_id for node_id, state in recovery_states.items() if not state.active}
            retryable_failed = {
                node_id
                for node_id, state in recovery_states.items()
                if state.retryable or self._is_retryable_compensation_failure(state.error)
            }
            if desired <= 0 or len(active) >= desired:
                return 0
            excluded = active | (failed - retryable_failed)
            with _infocenter_client(spec["infocenter_target"], timeout_sec=float(spec.get("timeout_sec", 10.0) or 10.0)) as infocenter:
                selected_nodes = list(
                    infocenter.select_task_nodes(
                        healthy_only=bool(spec.get("healthy_only", True)),
                        tags=list(spec.get("tags") or ()),
                        node_ids=list(spec.get("node_ids") or ()),
                        node_instance_ids=list(spec.get("node_instance_ids") or ()),
                        node_count=desired,
                        limit=max(desired * 2, int(spec.get("node_limit", 100) or 100)),
                        require_credit=False,
                        preferred_runtime_key="",
                        runtime=str(spec.get("runtime", "") or ""),
                    )
                )
            candidates = [
                node
                for node in selected_nodes
                if _node_instance_key_from_node(node) not in excluded
            ]
            current_node_instance_ids = {
                _node_instance_key_from_node(node) for node in selected_nodes if _node_instance_key_from_node(node)
            }
            candidate_node_instance_ids = {
                _node_instance_key_from_node(node) for node in candidates if _node_instance_key_from_node(node)
            }
            if self._compensation_deferred_by_retry_probe(
                resource_name=str(getattr(self, "pool_name", "") or getattr(self, "job_id", "") or ""),
                active=active,
                desired=desired,
                current_node_instance_ids=current_node_instance_ids,
                candidate_node_instance_ids=candidate_node_instance_ids,
            ):
                return 0
            if not candidates:
                return 0
            missing = max(0, desired - len(active))

            def _create_pool_on_node(node: InfoCenterNode) -> Tuple[str, InfoCenterNode, Optional[NativeTaskPoolClient], str]:
                node_key = _node_instance_key_from_node(node)
                client = None
                try:
                    target = _node_control_target_for_node(node)
                    client = _new_node_control_client(target, timeout_sec=float(spec.get("timeout_sec", 10.0) or 10.0))
                    pool = client.create_task_pool_from_bytes(
                        owner_client_id=str(spec.get("owner_client_id", "") or ""),
                        pool_name=str(spec.get("pool_name", "") or ""),
                        blob=spec.get("blob") or b"",
                        runtime=str(spec.get("runtime", "py3") or "py3"),
                        entry_module=str(spec.get("entry_module", "") or ""),
                        entry_callable=str(spec.get("entry_callable", "run") or "run"),
                        package_format=str(spec.get("package_format", "") or ""),
                        deps=spec.get("deps"),
                        managed_global_names=list(spec.get("managed_global_names") or ()),
                        initial_globals=dict(spec.get("initial_globals") or {}),
                        worker_count=max(1, int(spec.get("worker_count", 1) or 1)),
                        heartbeat_timeout_sec=get_taskpool_heartbeat_timeout_sec(int(spec.get("heartbeat_timeout_sec", 0) or 0)),
                        idle_ttl_sec=max(0, int(spec.get("idle_ttl_sec", 0) or 0)),
                        chunk_size=max(1, int(spec.get("chunk_size", OBJECT_CHUNK_SIZE_BYTES) or OBJECT_CHUNK_SIZE_BYTES)),
                        api_token=str(spec.get("api_token", "") or ""),
                        expected_node_instance_id=node_key,
                    )
                except Exception as exc:
                    if client is not None:
                        with contextlib.suppress(Exception):
                            client.close()
                    return node_key, node, None, repr(exc)
                pool.node_instance_id = node_key
                pool.node_id = str(node.node_id or "")
                return node_key, node, pool, ""

            added = 0
            for node in candidates[:missing]:
                node_key, node, pool, error_message = _create_pool_on_node(node)
                if error_message:
                    with self._pool_lock:
                        self.failures[node_key] = error_message
                    category = classify_error(error_message, resource_kind="task_pool").value
                    _mark_infocenter_node_lost_on_identity_mismatch(
                        infocenter_factory=_infocenter_client,
                        infocenter_target=str(spec["infocenter_target"]),
                        timeout_sec=float(spec.get("timeout_sec", 10.0) or 10.0),
                        node_instance_id=node_key,
                        error_message=error_message,
                        reason_prefix="task pool compensation identity mismatch",
                    )
                    logger.warning(
                        "task pool dynamic compensation create failed pool_name=%s "
                        "node_id=%s node_instance_id=%s control_addr=%s category=%s err=%s",
                        spec.get("pool_name", ""),
                        getattr(node, "node_id", ""),
                        node_key,
                        getattr(node, "control_addr", ""),
                        category,
                        error_message,
                    )
                    continue
                if pool is None:
                    continue
                with self._pool_lock:
                    if len(self._active_replica_snapshot()) >= desired:
                        _close_task_pool_replica(pool, reason="extra compensated task pool")
                        with contextlib.suppress(Exception):
                            pool._client.close()  # noqa: SLF001
                        continue
                    if node_key in self._pools:
                        if node_key in active or node_key not in retryable_failed:
                            _close_task_pool_replica(pool, reason="duplicate compensated task pool")
                            with contextlib.suppress(Exception):
                                pool._client.close()  # noqa: SLF001
                            continue
                        old_pool = self._pools.pop(node_key, None)
                        if old_pool is not None and old_pool is not pool:
                            _close_task_pool_replica(old_pool, reason="replace failed task pool replica")
                            with contextlib.suppress(Exception):
                                old_pool._client.close()  # noqa: SLF001
                    if not self._heartbeat_new_replica_before_activate(node_key, pool):
                        _close_task_pool_replica(pool, reason="compensated task pool initial heartbeat failed")
                        with contextlib.suppress(Exception):
                            pool._client.close()  # noqa: SLF001
                        continue
                    self._pools[node_key] = pool
                    self.nodes[node_key] = node
                    self.failures.pop(node_key, None)
                    self._active_nodes.add(node_key)
                    self._submit_breaker_states.setdefault(node_key, CandidateBreakerState())
                    added += 1
                    self._wake_keepalive()
            if added and self._last_managed_globals is not None:
                self.update_globals(dict(self._last_managed_globals))
            if added:
                _emit_taskpool_notice(
                    f"dynamic compensation added={added} target_nodes={desired} "
                    f"routes={_format_pool_route_summary(self.route_summary())}"
                )
            return added
        finally:
            self._compensation_lock.release()

    @property
    def methods(self) -> List[str]:
        return [self._task_method]

    @property
    def serialization_mode(self) -> str:
        return str(self._serialization_mode or "")

    def _ensure_method(self, method_name: str) -> str:
        normalized = str(method_name or "").strip()
        if not normalized:
            raise ValueError("TaskPool call requires method")
        if normalized != self._task_method:
            raise AttributeError(
                f"'{type(self).__name__}' has no method '{normalized}'. Available methods: {self.methods}"
            )
        return normalized

    def _next_task_id(self) -> str:
        with self._submit_lock:
            self._submit_seq += 1
            return f"{self.job_id}-task-{self._submit_seq:04d}"

    def _available_pool_node_ids(self) -> List[str]:
        ordered_node_ids = [str(node_id) for node_id in self._pools.keys()]
        if hasattr(self, "_active_nodes"):
            active_nodes = getattr(self, "_active_nodes", None)
            if active_nodes is getattr(self, "_active_replica_ids", None):
                active = self._active_replica_snapshot()
            else:
                active = {str(node_id) for node_id in list(active_nodes or []) if str(node_id)}
            return [
                node_id
                for node_id in ordered_node_ids
                if node_id in active and self._pool_candidate_allowed(node_id)
            ]
        if hasattr(self, "_active_replica_ids"):
            active = self._active_replica_snapshot()
            return [
                node_id
                for node_id in ordered_node_ids
                if node_id in active and self._pool_candidate_allowed(node_id)
            ]
        return [node_id for node_id in ordered_node_ids if self._pool_candidate_allowed(node_id)]

    def _pool_breaker_state(self, node_id: str) -> CandidateBreakerState:
        normalized = str(node_id or "").strip()
        state = self._submit_breaker_states.get(normalized)
        if state is None:
            state = CandidateBreakerState()
            self._submit_breaker_states[normalized] = state
        return state

    def _pool_candidate_allowed(self, node_id: str) -> bool:
        state = self._pool_breaker_state(node_id)
        _status, allowed = candidate_allowed(state)
        return allowed

    def _mark_pool_submit_success(self, node_id: str) -> None:
        mark_candidate_success(self._pool_breaker_state(node_id))
        self._scheduler_state.disabled_candidates.discard(str(node_id))
        self._scheduler_state.recent_submit_failures[str(node_id)] = 0

    def _mark_pool_submit_failure(self, node_id: str, *, failure_kind: str, error: object) -> None:
        mark_candidate_failure(
            self._pool_breaker_state(node_id),
            failure_kind=failure_kind,
            error=error,
            failure_threshold=2,
            cooldown_sec=1.0,
            max_cooldown_sec=10.0,
        )
        current = int(self._scheduler_state.recent_submit_failures.get(str(node_id), 0) or 0)
        self._scheduler_state.recent_submit_failures[str(node_id)] = current + 1
        self._scheduler_state.disabled_candidates.add(str(node_id))

    def _build_pool_scheduler_candidates(
        self,
        *,
        allowed_node_ids: Optional[Sequence[str]] = None,
        state: Optional[SchedulerState] = None,
    ) -> Tuple[List[SchedulerCandidate], SchedulerState]:
        selected_state = state or SchedulerState(
            local_inflight_by_candidate=dict(self._scheduler_state.local_inflight_by_candidate),
            disabled_candidates=set(self._scheduler_state.disabled_candidates),
            recent_submit_failures=dict(self._scheduler_state.recent_submit_failures),
        )
        allowed = set(str(node_id) for node_id in (allowed_node_ids or self._available_pool_node_ids()))
        active = set(self._available_pool_node_ids())
        candidates: List[SchedulerCandidate] = []
        for node_id in allowed:
            pool = self._pools.get(node_id)
            if pool is None:
                continue
            node = self.nodes.get(node_id)
            worker_capacity = max(1, int(getattr(pool, "worker_count", 0) or 1))
            node_inflight = max(0, int(getattr(node, "inflight", 0) or 0))
            alive_workers = max(
                0,
                int(getattr(node, "task_pool_worker_available", 0) or getattr(pool, "worker_count", 0) or 0),
            )
            predicted_busy = float(node_inflight) / float(max(1, alive_workers or worker_capacity))
            candidates.append(
                SchedulerCandidate(
                    id=str(node_id),
                    kind="task_pool",
                    node_id=str(getattr(node, "node_id", "") or node_id),
                    node_instance_id=str(node_id),
                    healthy=bool(node_id in active),
                    schedulable=True,
                    drain=False,
                    breaker_state=self._pool_breaker_state(node_id).state,
                    predicted_busy=predicted_busy,
                    node_inflight=node_inflight,
                    alive_workers=max(1, alive_workers or worker_capacity),
                    worker_capacity=worker_capacity,
                    credit=max(0, int(getattr(node, "credit", 0) or 0)),
                    recent_failures=int(selected_state.recent_submit_failures.get(str(node_id), 0) or 0),
                )
            )
        return candidates, selected_state

    def _effective_worker_count(self) -> int:
        total_workers = 0
        for node_id in self._available_pool_node_ids():
            pool = self._pools.get(node_id)
            worker_capacity = max(0, int(getattr(pool, "worker_count", 0) or 0))
            alive_workers = 0
            if pool is not None:
                with contextlib.suppress(Exception):
                    info = pool.get_status()
                    alive_workers = max(0, int(getattr(info, "alive_workers", 0) or 0))
                    worker_capacity = max(worker_capacity, int(getattr(info, "worker_count", 0) or 0))
            total_workers += max(alive_workers, worker_capacity, 0)
        return max(1, total_workers)

    def _default_max_in_flight(self) -> int:
        return max(1, int(math.ceil(float(self._effective_worker_count()) * _DEFAULT_MAX_IN_FLIGHT_WORKER_FACTOR)))

    def _resolve_max_in_flight(self, requested: Optional[int]) -> int:
        if requested is not None:
            try:
                normalized = int(requested)
            except Exception:
                normalized = 0
            if normalized > 0:
                return normalized
        return self._default_max_in_flight()

    def _plan_pool_node_targets(
        self,
        *,
        count: int,
        strategy: str,
        state: SchedulerState,
        allowed_node_ids: Optional[Sequence[str]] = None,
    ) -> List[str]:
        normalized_count = max(0, int(count or 0))
        if normalized_count <= 0:
            return []
        candidates, selected_state = self._build_pool_scheduler_candidates(
            allowed_node_ids=allowed_node_ids,
            state=state,
        )
        if not candidates:
            raise RuntimeError("task pool has no active node pools")
        profile = resolve_taskpool_strategy(strategy)
        with self._pool_lock:
            rr_start = self._pool_cycle
            self._pool_cycle += normalized_count
        planned: List[str] = []
        for offset in range(normalized_count):
            selected = select_one_candidate(
                candidates,
                profile=profile,
                state=selected_state,
                round_robin_counter=rr_start + offset,
            )
            target_node_id = str(selected.id)
            selected_state.local_inflight_by_candidate[target_node_id] = (
                int(selected_state.local_inflight_by_candidate.get(target_node_id, 0) or 0) + 1
            )
            planned.append(target_node_id)
        return planned

    def _build_task_submit_item(
        self,
        *,
        node_id: str,
        payload: Dict[str, object],
        task_id_prefix: str = "",
        timeout_hint_sec: int = 0,
        priority: int = 1,
        serialization_mode: str = "",
        use_transport_payload: bool = True,
    ) -> pb2.TaskSubmitItem:
        task_id = self._next_task_id()
        prefix = str(task_id_prefix or f"{self.job_id}-task").strip()
        if prefix:
            task_id = f"{prefix}-{task_id.rsplit('-', 1)[-1]}"
        prepared_payload = _prepare_task_payload_for_submit(
            self._pools[node_id]._client,  # noqa: SLF001
            dict(payload or {}),
            serialization_mode=serialization_mode or self._serialization_mode,
            effective_policy=self.effective_policy,
        )
        item_kwargs = {
            "task_id": task_id,
            "timeout_hint_sec": max(0, int(timeout_hint_sec)),
            "priority": max(1, int(priority)),
        }
        effective_mode = str(serialization_mode or self._serialization_mode or "").strip().lower() or "legacy_v1"
        if bool(use_transport_payload) and should_use_raw_bytes_payload(
            mode=effective_mode,
            effective_policy=self.effective_policy,
        ):
            item_kwargs["transport_payload"] = encode_transport_payload_bytes(
                prepared_payload,
                mode=effective_mode,
                context="taskpool_session",
                limit_bytes=self.effective_policy.inline_payload_hard_limit_bytes,
            )
        else:
            _, payload_struct, _ = serialize_inline_payload(
                prepared_payload,
                context="task pool payload",
                mode=effective_mode,
                limit_bytes=self.effective_policy.inline_payload_hard_limit_bytes,
            )
            item_kwargs["payload"] = payload_struct
        return pb2.TaskSubmitItem(**item_kwargs)

    def _build_task_submit_item_for_node(
        self,
        *,
        node_id: str,
        payload: Dict[str, object],
        task_id_prefix: str = "",
        timeout_hint_sec: int = 0,
        priority: int = 1,
        serialization_mode: str = "",
    ) -> pb2.TaskSubmitItem:
        pool_client = self._pools[node_id]._client  # noqa: SLF001
        if isinstance(pool_client, _DirectLocalTaskPoolNodeClient):
            task_id = self._next_task_id()
            prefix = str(task_id_prefix or f"{self.job_id}-task").strip()
            if prefix:
                task_id = f"{prefix}-{task_id.rsplit('-', 1)[-1]}"
            pool_client.put_direct_payload(task_id, dict(payload or {}))
            return pb2.TaskSubmitItem(
                task_id=task_id,
                timeout_hint_sec=max(0, int(timeout_hint_sec)),
                priority=max(1, int(priority)),
            )
        if self._is_local_session():
            return self._build_task_submit_item(
                node_id=node_id,
                payload=payload,
                task_id_prefix=task_id_prefix,
                timeout_hint_sec=timeout_hint_sec,
                priority=priority,
                serialization_mode=LOCAL_IPC_SERIALIZATION_MODE,
                use_transport_payload=True,
            )
        return self._build_task_submit_item(
            node_id=node_id,
            payload=payload,
            task_id_prefix=task_id_prefix,
            timeout_hint_sec=timeout_hint_sec,
            priority=priority,
            serialization_mode=serialization_mode,
            use_transport_payload=True,
        )

    def _register_pending_task_ids(self, accepted: Sequence[pb2.TaskAccepted], *, node_id: str = "") -> None:
        with self._result_state_lock:
            for item in accepted:
                task_id = str(item.task_id or "").strip()
                if not task_id:
                    continue
                self._pending_task_ids.add(task_id)
                if node_id:
                    normalized_node_id = str(node_id)
                    self._pending_task_node_ids[task_id] = normalized_node_id
                    self._scheduler_state.local_inflight_by_candidate[normalized_node_id] = (
                        int(self._scheduler_state.local_inflight_by_candidate.get(normalized_node_id, 0) or 0) + 1
                    )

    def _register_replay_record(
        self,
        *,
        logical_index: int,
        logical_key: object,
        original_payload: Dict[str, object],
        task_id: str,
        node_id: str,
        attempt: int,
        last_error: str = "",
        timeout_retry_count: int = 0,
    ) -> None:
        normalized_task_id = str(task_id or "").strip()
        normalized_node_id = str(node_id or "").strip()
        if not normalized_task_id:
            return
        record = _TaskReplayRecord(
            logical_index=int(logical_index),
            logical_key=logical_key,
            original_payload=dict(original_payload or {}),
            current_task_id=normalized_task_id,
            current_node_id=normalized_node_id,
            attempt=max(0, int(attempt or 0)),
            submitted_at=time.time(),
            last_error=str(last_error or ""),
            timeout_retry_count=max(0, int(timeout_retry_count or 0)),
        )
        with self._result_state_lock:
            self._replay_records[normalized_task_id] = record
            if normalized_node_id:
                self._replay_node_index.setdefault(normalized_node_id, set()).add(normalized_task_id)

    def _remove_replay_record_unlocked(self, task_id: str) -> Optional[_TaskReplayRecord]:
        normalized = str(task_id or "").strip()
        if not normalized:
            return None
        record = self._replay_records.pop(normalized, None)
        if record is None:
            return None
        node_tasks = self._replay_node_index.get(record.current_node_id)
        if node_tasks is not None:
            node_tasks.discard(normalized)
            if not node_tasks:
                self._replay_node_index.pop(record.current_node_id, None)
        return record

    def _take_replay_record(self, task_id: str) -> Optional[_TaskReplayRecord]:
        with self._result_state_lock:
            return self._remove_replay_record_unlocked(task_id)

    def _mark_taskpool_node_lost(self, node_id: str, *, error: object) -> Tuple[List[_TaskReplayRecord], List[str]]:
        normalized_node_id = str(node_id or "").strip()
        if not normalized_node_id:
            return [], []
        self._active_nodes.discard(normalized_node_id)
        self._scheduler_state.disabled_candidates.add(normalized_node_id)
        self.failures[normalized_node_id] = str(error or "task pool node lost")
        self._mark_pool_submit_failure(normalized_node_id, failure_kind=REMOTE_INFRA_FAILED, error=error)
        with self._result_state_lock:
            task_ids = set(self._replay_node_index.get(normalized_node_id, set()))
            task_ids.update(
                task_id
                for task_id, mapped_node_id in self._pending_task_node_ids.items()
                if str(mapped_node_id or "").strip() == normalized_node_id
            )
            records: List[_TaskReplayRecord] = []
            orphan_task_ids: List[str] = []
            for task_id in task_ids:
                self._pending_task_ids.discard(task_id)
                self._pending_task_node_ids.pop(task_id, None)
                record = self._remove_replay_record_unlocked(task_id)
                if record is not None:
                    records.append(record)
                else:
                    orphan_task_ids.append(str(task_id))
            self._scheduler_state.local_inflight_by_candidate[normalized_node_id] = 0
        return records, orphan_task_ids

    def _take_pending_replay_records_for_node(self, node_id: str) -> Tuple[List[_TaskReplayRecord], List[str]]:
        normalized_node_id = str(node_id or "").strip()
        if not normalized_node_id:
            return [], []
        with self._result_state_lock:
            task_ids = set(self._replay_node_index.get(normalized_node_id, set()))
            task_ids.update(
                task_id
                for task_id, mapped_node_id in self._pending_task_node_ids.items()
                if str(mapped_node_id or "").strip() == normalized_node_id
            )
            records: List[_TaskReplayRecord] = []
            orphan_task_ids: List[str] = []
            for task_id in task_ids:
                self._pending_task_ids.discard(task_id)
                self._pending_task_node_ids.pop(task_id, None)
                record = self._remove_replay_record_unlocked(task_id)
                if record is not None:
                    records.append(record)
                else:
                    orphan_task_ids.append(str(task_id))
            self._scheduler_state.local_inflight_by_candidate[normalized_node_id] = 0
        return records, orphan_task_ids

    @staticmethod
    def _local_failed_task_result(task_id: str, *, error_message: str) -> pb2.TaskResult:
        return pb2.TaskResult(
            task_id=str(task_id or ""),
            status=pb2.TASK_STATUS_FAILED_INFRA,
            error=pb2.TaskError(type="NodeInstanceLost", message=str(error_message or "node lost")),
        )

    def _fail_pending_tasks_for_lost_node(self, node_id: str, *, error: object) -> None:
        records, orphan_task_ids = self._mark_taskpool_node_lost(node_id, error=error)
        message = str(error or "node lost before task completed")
        failed_task_ids = [record.current_task_id for record in records] + list(orphan_task_ids)
        with self._result_state_lock:
            for task_id in failed_task_ids:
                self._pending_task_ids.add(task_id)
                self._pending_task_node_ids[task_id] = str(node_id or "")
                self._buffered_result_items.append(
                    (str(node_id or ""), self._local_failed_task_result(task_id, error_message=message))
                )
                self.node_lost_failed_tasks += 1

    def _submit_task_items_to_node(
        self,
        node_id: str,
        items: Sequence[pb2.TaskSubmitItem],
        *,
        job_id: str = "",
    ) -> pb2.SubmitTasksResponse:
        if not items:
            return pb2.SubmitTasksResponse(ok=True, accepted=[], rejected=[], node_credit=0)
        pool = self._pools[node_id]
        node = self.nodes.get(node_id)
        limit = _node_submit_http_body_limit(node, pool)
        safe_limit = max(0, int(limit * 0.8)) if limit > 0 else 0
        batches: List[List[pb2.TaskSubmitItem]] = []
        if safe_limit <= 0:
            batches = [list(items)]
        else:
            current: List[pb2.TaskSubmitItem] = []
            current_size = 1024
            for item in items:
                item_size = _task_submit_item_http_payload_size(item)
                if current and current_size + item_size > safe_limit:
                    batches.append(current)
                    current = []
                    current_size = 1024
                current.append(item)
                current_size += item_size
            if current:
                batches.append(current)
        accepted: List[pb2.TaskAccepted] = []
        rejected: List[pb2.TaskRejected] = []
        node_credit = 0
        for batch in batches:
            resp = pool.submit_tasks(batch, job_id=str(job_id or self.job_id).strip())
            self._register_pending_task_ids(resp.accepted, node_id=node_id)
            accepted.extend(resp.accepted)
            rejected.extend(resp.rejected)
            node_credit = int(resp.node_credit or node_credit or 0)
        return pb2.SubmitTasksResponse(ok=True, accepted=accepted, rejected=rejected, node_credit=node_credit)

    def _submit_grouped_task_items(
        self,
        grouped: Dict[str, List[pb2.TaskSubmitItem]],
        *,
        job_id: str = "",
    ) -> pb2.SubmitTasksResponse:
        accepted: List[pb2.TaskAccepted] = []
        rejected: List[pb2.TaskRejected] = []
        for node_id, items in grouped.items():
            resp = self._submit_task_items_to_node(node_id, items, job_id=job_id)
            accepted.extend(resp.accepted)
            rejected.extend(resp.rejected)
        return pb2.SubmitTasksResponse(ok=True, accepted=accepted, rejected=rejected, node_credit=0)

    def submit_payloads(
        self,
        payloads: Sequence[Dict[str, object]],
        *,
        task_method: str = "",
        strategy: str = "taskpool_default",
        timeout_sec: float = 60.0,
        job_id: str = "",
        task_id_prefix: str = "",
        timeout_hint_sec: int = 0,
        priority: int = 1,
        runtime_key: str = "",
        serialization_mode: str = "",
    ) -> pb2.SubmitTasksResponse:
        del timeout_sec, runtime_key
        if self._is_local_session():
            effective_serialization_mode = "legacy_v1"
        else:
            effective_serialization_mode = resolve_effective_serialization_mode(
                request_mode=serialization_mode,
                context="taskpool_session",
                frozen_mode=self._serialization_mode,
            )
        self._assert_session_available("submit_payloads")
        if self._closed:
            raise RuntimeError("TaskPool session is closed")
        self._ensure_method(str(task_method or self._task_method).strip() or self._task_method)
        temp_state = SchedulerState(
            local_inflight_by_candidate=dict(self._scheduler_state.local_inflight_by_candidate),
            disabled_candidates=set(self._scheduler_state.disabled_candidates),
            recent_submit_failures=dict(self._scheduler_state.recent_submit_failures),
        )
        grouped: Dict[str, List[pb2.TaskSubmitItem]] = {}
        planned_targets = self._plan_pool_node_targets(
            count=len(payloads),
            strategy=strategy,
            state=temp_state,
            allowed_node_ids=self._available_pool_node_ids(),
        )
        for payload, target_node_id in zip(payloads, planned_targets):
            grouped.setdefault(target_node_id, []).append(
                self._build_task_submit_item_for_node(
                    node_id=target_node_id,
                    payload=payload if isinstance(payload, dict) else {},
                    task_id_prefix=task_id_prefix,
                    timeout_hint_sec=max(0, int(timeout_hint_sec)),
                    priority=max(1, int(priority)),
                    serialization_mode=effective_serialization_mode,
                )
            )
        try:
            resp = self._submit_grouped_task_items(grouped, job_id=job_id)
        except Exception as exc:
            for node_id in grouped:
                self._mark_pool_submit_failure(node_id, failure_kind=SUBMIT_FAILED, error=exc)
            raise
        for node_id, items in grouped.items():
            if items:
                self._mark_pool_submit_success(node_id)
        return resp

    def _mark_result_consumed(self, task_id: str) -> None:
        normalized = str(task_id or "").strip()
        if not normalized:
            return
        with self._result_state_lock:
            self._pending_task_ids.discard(normalized)
            node_id = self._pending_task_node_ids.pop(normalized, "")
            self._remove_replay_record_unlocked(normalized)
            if node_id:
                current = int(self._scheduler_state.local_inflight_by_candidate.get(node_id, 0) or 0)
                self._scheduler_state.local_inflight_by_candidate[node_id] = max(0, current - 1)

    def _pending_result_count(self) -> int:
        with self._result_state_lock:
            return len(self._pending_task_ids)

    def _pending_result_count_by_node(self) -> Dict[str, int]:
        with self._result_state_lock:
            counts: Dict[str, int] = {}
            for task_id in self._pending_task_ids:
                node_id = str(self._pending_task_node_ids.get(task_id, "") or "<unmapped>")
                counts[node_id] = int(counts.get(node_id, 0) or 0) + 1
            return counts

    def _is_pending_task_id_unlocked(self, task_id: str) -> bool:
        normalized = str(task_id or "").strip()
        if not normalized:
            return False
        return normalized in self._pending_task_ids

    def _is_pending_task_id(self, task_id: str) -> bool:
        with self._result_state_lock:
            return self._is_pending_task_id_unlocked(task_id)

    def _clear_pending_for_current_job(self) -> None:
        with self._result_state_lock:
            self._pending_task_ids.clear()
            self._pending_task_node_ids.clear()
            self._buffered_result_items.clear()
            self._replay_records.clear()
            self._replay_node_index.clear()
            self._scheduler_state.local_inflight_by_candidate.clear()

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
                if not self._is_pending_task_id_unlocked(normalized):
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

    def _task_result_to_item(self, node_id: str, task_result: pb2.TaskResult) -> ExecutionItem:
        resolved_node_id = str(self.nodes.get(node_id).node_id if node_id in self.nodes else node_id)
        if int(task_result.status) != int(pb2.TASK_STATUS_SUCCEEDED):
            error = task_result.error
            return ExecutionItem(
                index=-1,
                ok=False,
                result=None,
                error_type=str(error.type or ""),
                error_message=str(error.message or f"task failed: {task_result.task_id}"),
                node_id=resolved_node_id,
                key=str(task_result.task_id or ""),
                status=int(task_result.status),
                task_id=str(task_result.task_id or ""),
                node_instance_id=str(node_id),
            )
        try:
            data = self._pools[node_id]._client.fetch_result_data(task_result)  # noqa: SLF001
            return ExecutionItem(
                index=-1,
                ok=True,
                result=data,
                node_id=resolved_node_id,
                key=str(task_result.task_id or ""),
                status=int(task_result.status),
                task_id=str(task_result.task_id or ""),
                node_instance_id=str(node_id),
            )
        except Exception as exc:
            return ExecutionItem(
                index=-1,
                ok=False,
                result=None,
                error_type=exc.__class__.__name__,
                error_message=str(exc),
                node_id=resolved_node_id,
                key=str(task_result.task_id or ""),
                status=int(task_result.status),
                task_id=str(task_result.task_id or ""),
                node_instance_id=str(node_id),
            )

    def _merge_payloads_with_shared_kwargs(
        self,
        payloads: Iterable[Dict[str, object]],
        *,
        shared_kwargs: Optional[Dict[str, object]] = None,
    ) -> Iterator[Dict[str, object]]:
        shared = dict(shared_kwargs or {})
        for payload in payloads:
            if not isinstance(payload, dict):
                raise TypeError("payloads must be mapping payloads")
            yield {**payload, **shared}

    def _item_with_index(self, item: ExecutionItem, *, index: int, key: Union[int, str]) -> ExecutionItem:
        return replace(item, index=int(index), key=key)

    @staticmethod
    def _resolve_server_wait_ms(*, server_wait_ms: Optional[int], wait_ms: int) -> int:
        """Resolve the preferred server-side wait alias without changing authority."""
        return max(0, int(server_wait_ms if server_wait_ms is not None else wait_ms))

    def _iter_raw_results(
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
        poll_round = 0
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
            node_items = list(self._pools.items())
            if node_items:
                start_index = poll_round % len(node_items)
                ordered_items = node_items[start_index:] + node_items[:start_index]
            else:
                ordered_items = []
            blocking_node_id = ""
            if ordered_items:
                blocking_node_id = str(ordered_items[0][0])
                pending_node_ids = {
                    str(node_id)
                    for node_id in self._pending_task_node_ids.values()
                    if str(node_id)
                }
                for node_id, _pool in ordered_items:
                    if str(node_id) in pending_node_ids:
                        blocking_node_id = str(node_id)
                        break
                poll_round += 1
            for node_id, pool in ordered_items:
                per_pull_limit = max(1, int(limit or 100))
                if remaining_by_max > 0:
                    per_pull_limit = max(1, min(per_pull_limit, remaining_by_max))
                per_pull_wait_ms = max(0, int(wait_ms or 0)) if str(node_id) == blocking_node_id else 0
                try:
                    resp = pool.pull_results(limit=per_pull_limit, wait_ms=per_pull_wait_ms, cursor="")
                except Exception as exc:
                    self._fail_pending_tasks_for_lost_node(node_id, error=exc)
                    any_result = True
                    continue
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
        server_wait_ms: Optional[int] = None,
        wait_ms: int = 500,
        limit: int = 100,
        job_id: str = "",
    ) -> Iterator[pb2.TaskResult]:
        self._assert_session_available("iter_results")
        effective_wait_ms = self._resolve_server_wait_ms(server_wait_ms=server_wait_ms, wait_ms=wait_ms)
        for _node_id, item in self._iter_raw_results(
            max_count=max_count,
            timeout_sec=timeout_sec,
            wait_ms=effective_wait_ms,
            limit=limit,
            job_id=job_id,
        ):
            yield item

    def collect_results(
        self,
        *,
        max_count: Optional[int] = None,
        timeout_sec: float = 30.0,
        server_wait_ms: Optional[int] = None,
        wait_ms: int = 500,
        limit: int = 100,
        job_id: str = "",
    ) -> List[pb2.TaskResult]:
        wait_kwargs = {"wait_ms": wait_ms}
        if server_wait_ms is not None:
            wait_kwargs["server_wait_ms"] = server_wait_ms
        return list(
            self.iter_results(
                max_count=max_count,
                timeout_sec=timeout_sec,
                limit=limit,
                job_id=job_id,
                **wait_kwargs,
            )
        )

    def iter_data(
        self,
        *,
        max_count: Optional[int] = None,
        timeout_sec: float = 30.0,
        server_wait_ms: Optional[int] = None,
        wait_ms: int = 500,
        limit: int = 100,
        job_id: str = "",
        raise_on_error: bool = False,
        task_ids: Optional[Set[str]] = None,
    ) -> Iterator[Tuple[str, Any]]:
        self._assert_session_available("iter_data")
        for item in self._iter_execution_items(
            max_count=max_count,
            timeout_sec=timeout_sec,
            wait_ms=self._resolve_server_wait_ms(server_wait_ms=server_wait_ms, wait_ms=wait_ms),
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
        server_wait_ms: Optional[int] = None,
        wait_ms: int = 500,
        limit: int = 100,
        job_id: str = "",
        raise_on_error: bool = False,
        task_ids: Optional[Set[str]] = None,
    ) -> List[Tuple[str, Any]]:
        wait_kwargs = {"wait_ms": wait_ms}
        if server_wait_ms is not None:
            wait_kwargs["server_wait_ms"] = server_wait_ms
        return list(
            self.iter_data(
                max_count=max_count,
                timeout_sec=timeout_sec,
                limit=limit,
                job_id=job_id,
                raise_on_error=raise_on_error,
                task_ids=task_ids,
                **wait_kwargs,
            )
        )

    def update_globals(self, values: Dict[str, object]) -> str:
        if self._closed:
            raise RuntimeError("task pool session is closed")
        pools_snapshot = list(self._pools.items())
        active_clients = [pool._client for _node_id, pool in pools_snapshot]  # noqa: SLF001

        def _update_batch(
            _node_id: str,
            pool: NativeTaskPoolClient,
            prepared_values: Dict[str, object],
            values_struct: object,
            transport_values: Optional[pb2.TransportPayload],
        ) -> object:
            update_encoded = getattr(pool._client, "update_runtime_globals_encoded", None)  # noqa: SLF001
            if callable(update_encoded):
                return update_encoded(
                    client_id=pool.pool_id,
                    code_version=pool.code_version,
                    runtime_key=pool.pool_id,
                    code_token=pool.pool_token,
                    prepared_keys=sorted(str(key) for key in prepared_values.keys()),
                    values=values_struct,
                    transport_values=transport_values,
                )
            return pool._client.update_runtime_globals_prepared(  # noqa: SLF001
                client_id=pool.pool_id,
                code_version=pool.code_version,
                runtime_key=pool.pool_id,
                code_token=pool.pool_token,
                prepared_values=prepared_values,
                serialization_mode=self._serialization_mode,
                effective_policy=self.effective_policy,
            )

        digests, failed_nodes = update_managed_globals_across_replicas(
            upload_clients=active_clients,
            values=values,
            targets=pools_snapshot,
            serialization_mode=self._serialization_mode,
            effective_policy=self.effective_policy,
            context="taskpool_session",
            thread_name_prefix="taskpool-update-globals",
            update_batch=_update_batch,
            include_empty_digest=False,
        )

        if not digests:
            raise RuntimeError(f"update_globals failed on all nodes: {failed_nodes}")
        self.globals_digests = dict(digests)
        self._last_managed_globals = dict(values or {})
        unique = {digest for digest in digests.values() if str(digest).strip()}
        return next(iter(unique), "") if len(unique) == 1 else next(iter(digests.values()))

    def _iter_execution_items(
        self,
        *,
        max_count: Optional[int] = None,
        timeout_sec: float = 30.0,
        wait_ms: int = 500,
        limit: int = 100,
        job_id: str = "",
        task_ids: Optional[Set[str]] = None,
    ) -> Iterator[ExecutionItem]:
        self._assert_session_available("iter_items")
        for node_id, task_result in self._iter_raw_results(
            max_count=max_count,
            timeout_sec=timeout_sec,
            wait_ms=wait_ms,
            limit=limit,
            job_id=job_id,
            task_ids=task_ids,
        ):
            yield self._task_result_to_item(node_id, task_result)

    def iter_items(
        self,
        payloads: Optional[Iterable[Dict[str, object]]] = None,
        *,
        task_method: str = "",
        strategy: str = "taskpool_default",
        max_in_flight: Optional[int] = None,
        timeout_sec: float = 30.0,
        max_count: Optional[int] = None,
        server_wait_ms: Optional[int] = None,
        wait_ms: int = 500,
        limit: int = 100,
        job_id: str = "",
        task_ids: Optional[Set[str]] = None,
        progress: ProgressOption = False,
        progress_interval_sec: float = 2.0,
        **shared_kwargs,
    ) -> Iterator[ExecutionItem]:
        """Iterate task results as structured items.

        When ``payloads is None``, this only consumes already-submitted results from the current
        session. When ``payloads`` is provided, this submits that batch and yields `ExecutionItem`
        objects for the batch.
        """
        self._assert_session_available("iter_items")
        effective_wait_ms = self._resolve_server_wait_ms(server_wait_ms=server_wait_ms, wait_ms=wait_ms)
        if not is_progress_option(progress):
            shared_kwargs = {"progress": progress, **shared_kwargs}
            progress = False
        if payloads is None:
            if progress or callable(progress):
                completed = 0
                succeeded = 0
                failed = 0
                last_error = ""
                progress_total = max(0, int(max_count or 0)) if max_count is not None else 0
                reporter = ProgressReporter(
                    progress,
                    label=f"taskpool.{self._task_method}",
                    total=progress_total,
                    interval_sec=progress_interval_sec,
                )
                reporter.emit(phase="running", completed=0, succeeded=0, failed=0, submitted=progress_total, force=True)
                try:
                    for item in self._iter_execution_items(
                        max_count=max_count,
                        timeout_sec=timeout_sec,
                        wait_ms=effective_wait_ms,
                        limit=limit,
                        job_id=job_id,
                        task_ids=task_ids,
                    ):
                        completed += 1
                        if item.ok:
                            succeeded += 1
                        else:
                            failed += 1
                            last_error = str(item.error_message or item.error_type or "")
                        reporter.emit(
                            phase="running",
                            completed=completed,
                            succeeded=succeeded,
                            failed=failed,
                            submitted=progress_total,
                            last_error=last_error,
                        )
                        yield item
                except Exception as exc:
                    reporter.emit(
                        phase="failed",
                        completed=completed,
                        succeeded=succeeded,
                        failed=failed,
                        submitted=progress_total,
                        last_error=str(exc),
                        force=True,
                    )
                    raise
                reporter.done(completed=completed, succeeded=succeeded, failed=failed, submitted=progress_total, last_error=last_error)
                return
            yield from self._iter_execution_items(
                max_count=max_count,
                timeout_sec=timeout_sec,
                wait_ms=effective_wait_ms,
                limit=limit,
                job_id=job_id,
                task_ids=task_ids,
            )
            return
        normalized_payloads = self._merge_payloads_with_shared_kwargs(payloads, shared_kwargs=dict(shared_kwargs))
        resolved_max_in_flight = self._resolve_max_in_flight(max_in_flight)
        progress_kwargs = {}
        if progress:
            progress_kwargs["progress"] = progress
            progress_kwargs["progress_interval_sec"] = progress_interval_sec
        for item in self.imap_unordered(
            normalized_payloads,
            task_method=task_method,
            strategy=strategy,
            max_in_flight=resolved_max_in_flight,
            receive_batch=max(1, min(resolved_max_in_flight, 32)),
            submit_timeout_sec=max(0.1, float(timeout_sec)),
            result_timeout_sec=max(0.1, float(timeout_sec)),
            server_wait_ms=effective_wait_ms,
            wait_ms=wait_ms,
            raise_on_error=False,
            node_window_factor=2.0,
            return_items=True,
            **progress_kwargs,
        ):
            if not isinstance(item, ExecutionItem):
                index, result = item
                yield ExecutionItem(
                    index=int(index),
                    ok=result is not None,
                    result=result,
                    error_type="" if result is not None else "TaskFailed",
                    error_message="" if result is not None else "task failed",
                    key=int(index),
                )
                continue
            yield item

    def collect_items(
        self,
        payloads: Optional[Iterable[Dict[str, object]]] = None,
        *,
        task_method: str = "",
        strategy: str = "taskpool_default",
        max_in_flight: Optional[int] = None,
        timeout_sec: float = 30.0,
        max_count: Optional[int] = None,
        server_wait_ms: Optional[int] = None,
        wait_ms: int = 500,
        limit: int = 100,
        job_id: str = "",
        task_ids: Optional[Set[str]] = None,
        progress: ProgressOption = False,
        progress_interval_sec: float = 2.0,
        **shared_kwargs,
    ) -> List[ExecutionItem]:
        wait_kwargs = {"wait_ms": wait_ms}
        if server_wait_ms is not None:
            wait_kwargs["server_wait_ms"] = server_wait_ms
        if not is_progress_option(progress):
            shared_kwargs = {"progress": progress, **shared_kwargs}
            progress = False
        progress_kwargs = {}
        if progress:
            progress_kwargs["progress"] = progress
            progress_kwargs["progress_interval_sec"] = progress_interval_sec
        items = list(
            self.iter_items(
                payloads,
                task_method=task_method,
                strategy=strategy,
                max_in_flight=max_in_flight,
                max_count=max_count,
                timeout_sec=timeout_sec,
                limit=limit,
                job_id=job_id,
                task_ids=task_ids,
                **wait_kwargs,
                **progress_kwargs,
                **shared_kwargs,
            )
        )
        if payloads is None:
            return items
        return sorted(items, key=lambda item: int(item.index))

    async def aiter_items(
        self,
        payloads: Optional[Iterable[Dict[str, object]]] = None,
        *,
        task_method: str = "",
        strategy: str = "taskpool_default",
        max_in_flight: Optional[int] = None,
        timeout_sec: float = 30.0,
        max_count: Optional[int] = None,
        server_wait_ms: Optional[int] = None,
        wait_ms: int = 500,
        limit: int = 100,
        job_id: str = "",
        task_ids: Optional[Set[str]] = None,
        progress: ProgressOption = False,
        progress_interval_sec: float = 2.0,
        **shared_kwargs,
    ) -> AsyncIterator[ExecutionItem]:
        """Async counterpart of :meth:`iter_items` with the same dual-mode semantics."""
        wait_kwargs = {"wait_ms": wait_ms}
        if server_wait_ms is not None:
            wait_kwargs["server_wait_ms"] = server_wait_ms
        if not is_progress_option(progress):
            shared_kwargs = {"progress": progress, **shared_kwargs}
            progress = False
        progress_kwargs = {}
        if progress:
            progress_kwargs["progress"] = progress
            progress_kwargs["progress_interval_sec"] = progress_interval_sec
        iterator = self.iter_items(
            payloads,
            task_method=task_method,
            strategy=strategy,
            max_in_flight=max_in_flight,
            timeout_sec=timeout_sec,
            max_count=max_count,
            limit=limit,
            job_id=job_id,
            task_ids=task_ids,
            **wait_kwargs,
            **progress_kwargs,
            **shared_kwargs,
        )
        async for item in _aiter_from_sync_iterator(iterator):
            yield item

    async def acollect_items(
        self,
        payloads: Optional[Iterable[Dict[str, object]]] = None,
        *,
        task_method: str = "",
        strategy: str = "taskpool_default",
        max_in_flight: Optional[int] = None,
        timeout_sec: float = 30.0,
        max_count: Optional[int] = None,
        server_wait_ms: Optional[int] = None,
        wait_ms: int = 500,
        limit: int = 100,
        job_id: str = "",
        task_ids: Optional[Set[str]] = None,
        progress: ProgressOption = False,
        progress_interval_sec: float = 2.0,
        **shared_kwargs,
    ) -> List[ExecutionItem]:
        """Async counterpart of :meth:`collect_items` with the same dual-mode semantics."""
        wait_kwargs = {"wait_ms": wait_ms}
        if server_wait_ms is not None:
            wait_kwargs["server_wait_ms"] = server_wait_ms
        if not is_progress_option(progress):
            shared_kwargs = {"progress": progress, **shared_kwargs}
            progress = False
        progress_kwargs = {}
        if progress:
            progress_kwargs["progress"] = progress
            progress_kwargs["progress_interval_sec"] = progress_interval_sec
        return await asyncio.to_thread(
            lambda: self.collect_items(
                payloads,
                task_method=task_method,
                strategy=strategy,
                max_in_flight=max_in_flight,
                timeout_sec=timeout_sec,
                max_count=max_count,
                limit=limit,
                job_id=job_id,
                task_ids=task_ids,
                **wait_kwargs,
                **progress_kwargs,
                **shared_kwargs,
            )
        )

    def _collect_data_for_task_ids(self, task_ids: Set[str], *, timeout_sec: float = 30.0) -> List[Tuple[str, Any]]:
        out: List[Tuple[str, Any]] = []
        for node_id, item in self._iter_raw_results(max_count=len(task_ids), timeout_sec=timeout_sec, task_ids=set(task_ids)):
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
        server_wait_ms: Optional[int] = None,
        wait_ms: int = 500,
        limit: int = 100,
        job_id: str = "",
        progress: ProgressOption = False,
        progress_interval_sec: float = 2.0,
    ) -> Sequence[pb2.TaskResult]:
        max_count = max(0, int(expected_count or 0))
        wait_kwargs = {"wait_ms": wait_ms}
        if server_wait_ms is not None:
            wait_kwargs["server_wait_ms"] = server_wait_ms
        reporter = ProgressReporter(
            progress,
            label=f"taskpool.{self._task_method}",
            total=max_count,
            interval_sec=progress_interval_sec,
        )
        results: List[pb2.TaskResult] = []
        succeeded = 0
        failed = 0
        last_error = ""
        reporter.emit(phase="running", completed=0, succeeded=0, failed=0, submitted=max_count, force=True)
        try:
            for item in self.iter_results(
                max_count=(max_count if max_count > 0 else None),
                timeout_sec=timeout_sec,
                limit=limit,
                job_id=job_id,
                **wait_kwargs,
            ):
                results.append(item)
                if int(item.status) == int(pb2.TASK_STATUS_SUCCEEDED):
                    succeeded += 1
                else:
                    failed += 1
                    last_error = str(getattr(getattr(item, "error", None), "message", "") or item.task_id or "")
                reporter.emit(
                    phase="running",
                    completed=len(results),
                    succeeded=succeeded,
                    failed=failed,
                    submitted=max_count,
                    last_error=last_error,
                )
        except Exception as exc:
            last_error = str(exc)
            reporter.emit(
                phase="failed",
                completed=len(results),
                succeeded=succeeded,
                failed=failed,
                submitted=max_count,
                last_error=last_error,
                force=True,
            )
            raise
        reporter.done(
            completed=len(results),
            succeeded=succeeded,
            failed=failed,
            submitted=max_count,
            last_error=last_error,
        )
        return results

    def wait_for_data(
        self,
        *,
        expected_count: int = 0,
        timeout_sec: float = 30.0,
        server_wait_ms: Optional[int] = None,
        wait_ms: int = 500,
        progress: ProgressOption = False,
        progress_interval_sec: float = 2.0,
    ) -> Sequence[Any]:
        max_count = max(0, int(expected_count or 0))
        wait_kwargs = {"wait_ms": wait_ms}
        if server_wait_ms is not None:
            wait_kwargs["server_wait_ms"] = server_wait_ms
        reporter = ProgressReporter(
            progress,
            label=f"taskpool.{self._task_method}",
            total=max_count,
            interval_sec=progress_interval_sec,
        )
        data_items: List[Any] = []
        last_error = ""
        reporter.emit(phase="running", completed=0, succeeded=0, failed=0, submitted=max_count, force=True)
        try:
            for _task_id, data in self.iter_data(
                max_count=(max_count if max_count > 0 else None),
                timeout_sec=timeout_sec,
                raise_on_error=True,
                **wait_kwargs,
            ):
                data_items.append(data)
                reporter.emit(
                    phase="running",
                    completed=len(data_items),
                    succeeded=len(data_items),
                    failed=0,
                    submitted=max_count,
                )
        except Exception as exc:
            last_error = str(exc)
            reporter.emit(
                phase="failed",
                completed=len(data_items),
                succeeded=len(data_items),
                failed=1,
                submitted=max_count,
                last_error=last_error,
                force=True,
            )
            raise
        reporter.done(
            completed=len(data_items),
            succeeded=len(data_items),
            failed=0,
            submitted=max_count,
            last_error=last_error,
        )
        return data_items

    def submit_values(
        self,
        values: Iterable[Any],
        *,
        arg_name: str = "value",
        task_method: str = "",
        strategy: str = "taskpool_default",
        serialization_mode: str = "",
        **shared_kwargs,
    ) -> pb2.SubmitTasksResponse:
        normalized_arg = str(arg_name or "value").strip() or "value"
        shared = dict(shared_kwargs)
        chunk_size = max(1, self._resolve_max_in_flight(None))
        accepted: List[pb2.TaskAccepted] = []
        rejected: List[pb2.TaskRejected] = []
        ok = True
        chunk: List[Dict[str, object]] = []
        for value in values:
            chunk.append({normalized_arg: value, **shared})
            if len(chunk) < chunk_size:
                continue
            resp = self.submit_payloads(
                chunk,
                task_method=task_method,
                strategy=strategy,
                serialization_mode=serialization_mode,
            )
            ok = ok and bool(resp.ok)
            accepted.extend(resp.accepted)
            rejected.extend(resp.rejected)
            chunk = []
        if chunk:
            resp = self.submit_payloads(
                chunk,
                task_method=task_method,
                strategy=strategy,
                serialization_mode=serialization_mode,
            )
            ok = ok and bool(resp.ok)
            accepted.extend(resp.accepted)
            rejected.extend(resp.rejected)
        return pb2.SubmitTasksResponse(ok=ok, accepted=accepted, rejected=rejected, node_credit=0)

    def _plan_limited_pool_node_targets(
        self,
        available_by_node: Dict[str, int],
        *,
        node_order: Sequence[str],
        max_new_tasks: int,
        profile,
        inflight_by_node: Dict[str, int],
        disabled_submit_nodes: Set[str],
        infra_failures_by_node: Dict[str, int],
        round_robin_start: int,
    ) -> List[str]:
        if max_new_tasks <= 0:
            return []
        remaining = {
            node_id: max(0, int(available_by_node.get(node_id, 0) or 0))
            for node_id in node_order
            if node_id not in disabled_submit_nodes
        }
        allowed = [node_id for node_id in node_order if remaining.get(node_id, 0) > 0]
        if not allowed:
            return []
        state = SchedulerState(
            local_inflight_by_candidate=dict(inflight_by_node),
            disabled_candidates=set(disabled_submit_nodes),
            recent_submit_failures=dict(infra_failures_by_node),
        )
        candidates, state = self._build_pool_scheduler_candidates(
            allowed_node_ids=allowed,
            state=state,
        )
        planned: List[str] = []
        rr_counter = int(round_robin_start or 0)
        while max_new_tasks > 0:
            if not any(remaining.get(node_id, 0) > 0 for node_id in node_order):
                break
            try:
                selected = select_one_candidate(
                    candidates,
                    profile=profile,
                    state=state,
                    round_robin_counter=rr_counter,
                )
            except RuntimeError:
                break
            node_id = str(selected.id)
            if remaining.get(node_id, 0) <= 0:
                state.disabled_candidates.add(node_id)
                continue
            rr_counter += 1
            planned.append(node_id)
            remaining[node_id] = max(0, int(remaining.get(node_id, 0) or 0) - 1)
            state.local_inflight_by_candidate[node_id] = (
                int(state.local_inflight_by_candidate.get(node_id, 0) or 0) + 1
            )
            if remaining[node_id] <= 0:
                state.disabled_candidates.add(node_id)
            max_new_tasks -= 1
        return planned

    def _replay_failure_item(self, record: _TaskReplayRecord, *, message: str) -> ExecutionItem:
        self.task_retry_exhausted_count += 1
        self.node_lost_failed_tasks += 1
        return ExecutionItem(
            index=int(record.logical_index),
            ok=False,
            result=None,
            error_type="NodeInstanceLost",
            error_message=str(message or "node lost before task completed"),
            node_id=str(getattr(self.nodes.get(record.current_node_id), "node_id", "") or record.current_node_id),
            key=record.logical_key,
            status=int(pb2.TASK_STATUS_FAILED_INFRA),
            task_id=str(record.current_task_id),
            node_instance_id=str(record.current_node_id),
        )

    def _retry_replay_record(
        self,
        record: _TaskReplayRecord,
        *,
        reason: str,
        disabled_submit_nodes: Set[str],
        scheduler_failures: Dict[str, str],
        inflight_by_node: Dict[str, int],
        task_index_by_id: Dict[str, int],
        max_infra_retries: int,
        retry_backoff_ms: int,
    ) -> Optional[ExecutionItem]:
        if int(record.attempt or 0) >= max(0, int(max_infra_retries or 0)):
            return self._replay_failure_item(record, message=reason or "node lost before task completed")
        candidates = [
            node_id
            for node_id in self._available_pool_node_ids()
            if node_id != record.current_node_id and node_id not in disabled_submit_nodes
        ]
        if not candidates:
            return self._replay_failure_item(record, message="no healthy task pool node available for retry")
        if retry_backoff_ms > 0:
            time.sleep(max(0.0, float(retry_backoff_ms) / 1000.0))
        last_error = reason or "node lost before task completed"
        for target_node_id in candidates:
            try:
                prepare_start = time.perf_counter()
                item = self._build_task_submit_item_for_node(
                    node_id=target_node_id,
                    payload=dict(record.original_payload or {}),
                    timeout_hint_sec=0,
                    priority=1,
                )
                self.retry_prepare_payload_ms += (time.perf_counter() - prepare_start) * 1000.0
                submit_start = time.perf_counter()
                resp = self._submit_task_items_to_node(target_node_id, [item], job_id=self.job_id)
                self.retry_submit_ms += (time.perf_counter() - submit_start) * 1000.0
                new_task_id = str(item.task_id or "").strip()
                accepted_ids = {
                    str(accepted.task_id or "").strip()
                    for accepted in resp.accepted
                    if str(accepted.task_id or "").strip()
                }
                if new_task_id not in accepted_ids:
                    raise RuntimeError("retry submit was not accepted")
            except Exception as exc:
                last_error = repr(exc)
                disabled_submit_nodes.add(target_node_id)
                scheduler_failures[target_node_id] = last_error
                self._mark_pool_submit_failure(target_node_id, failure_kind=SUBMIT_FAILED, error=exc)
                continue
            self.task_retry_count += 1
            self.task_retry_success_count += 1
            self.node_lost_replayed_tasks += 1
            task_index_by_id.pop(str(record.current_task_id or ""), None)
            task_index_by_id[new_task_id] = int(record.logical_index)
            self._register_replay_record(
                logical_index=int(record.logical_index),
                logical_key=record.logical_key,
                original_payload=dict(record.original_payload or {}),
                task_id=new_task_id,
                node_id=target_node_id,
                attempt=int(record.attempt or 0) + 1,
                last_error=reason,
            )
            inflight_by_node[target_node_id] = int(inflight_by_node.get(target_node_id, 0) or 0) + 1
            self._mark_pool_submit_success(target_node_id)
            return None
        return self._replay_failure_item(record, message=last_error)

    def _retry_replay_records(
        self,
        records: Sequence[_TaskReplayRecord],
        *,
        reason: str,
        disabled_submit_nodes: Set[str],
        scheduler_failures: Dict[str, str],
        inflight_by_node: Dict[str, int],
        task_index_by_id: Dict[str, int],
        max_infra_retries: int,
        retry_backoff_ms: int,
    ) -> List[ExecutionItem]:
        failed: List[ExecutionItem] = []
        for record in records:
            item = self._retry_replay_record(
                record,
                reason=reason,
                disabled_submit_nodes=disabled_submit_nodes,
                scheduler_failures=scheduler_failures,
                inflight_by_node=inflight_by_node,
                task_index_by_id=task_index_by_id,
                max_infra_retries=max_infra_retries,
                retry_backoff_ms=retry_backoff_ms,
            )
            if item is not None:
                failed.append(item)
        return failed

    def _retry_timeout_replay_records_evenly(
        self,
        records: Sequence[_TaskReplayRecord],
        *,
        reason: str,
        target_node_ids: Sequence[str],
        disabled_submit_nodes: Set[str],
        scheduler_failures: Dict[str, str],
        inflight_by_node: Dict[str, int],
        task_index_by_id: Dict[str, int],
    ) -> List[ExecutionItem]:
        failed: List[ExecutionItem] = []
        targets = [node_id for node_id in target_node_ids if node_id not in disabled_submit_nodes]
        if not targets:
            return [self._replay_failure_item(record, message="no healthy task pool node available for timeout retry") for record in records]
        target_offset = 0
        for record in records:
            if int(record.timeout_retry_count or 0) >= 1:
                failed.append(self._replay_failure_item(record, message=reason or "task result timeout after retry"))
                continue
            last_error = reason or "task result timeout"
            accepted = False
            attempted = 0
            record_targets = [node_id for node_id in targets if node_id != record.current_node_id] or list(targets)
            while attempted < len(record_targets):
                target_node_id = record_targets[(target_offset + attempted) % len(record_targets)]
                attempted += 1
                if target_node_id in disabled_submit_nodes:
                    continue
                try:
                    prepare_start = time.perf_counter()
                    item = self._build_task_submit_item_for_node(
                        node_id=target_node_id,
                        payload=dict(record.original_payload or {}),
                        timeout_hint_sec=0,
                        priority=1,
                    )
                    self.retry_prepare_payload_ms += (time.perf_counter() - prepare_start) * 1000.0
                    submit_start = time.perf_counter()
                    resp = self._submit_task_items_to_node(target_node_id, [item], job_id=self.job_id)
                    self.retry_submit_ms += (time.perf_counter() - submit_start) * 1000.0
                    new_task_id = str(item.task_id or "").strip()
                    accepted_ids = {
                        str(accepted_item.task_id or "").strip()
                        for accepted_item in resp.accepted
                        if str(accepted_item.task_id or "").strip()
                    }
                    if new_task_id not in accepted_ids:
                        raise RuntimeError("timeout retry submit was not accepted")
                except Exception as exc:
                    last_error = repr(exc)
                    disabled_submit_nodes.add(target_node_id)
                    scheduler_failures[target_node_id] = last_error
                    self._mark_pool_submit_failure(target_node_id, failure_kind=SUBMIT_FAILED, error=exc)
                    continue
                self.task_retry_count += 1
                self.task_retry_success_count += 1
                self.node_lost_replayed_tasks += 1
                task_index_by_id.pop(str(record.current_task_id or ""), None)
                task_index_by_id[new_task_id] = int(record.logical_index)
                self._register_replay_record(
                    logical_index=int(record.logical_index),
                    logical_key=record.logical_key,
                    original_payload=dict(record.original_payload or {}),
                    task_id=new_task_id,
                    node_id=target_node_id,
                    attempt=int(record.attempt or 0),
                    last_error=reason,
                    timeout_retry_count=int(record.timeout_retry_count or 0) + 1,
                )
                inflight_by_node[target_node_id] = int(inflight_by_node.get(target_node_id, 0) or 0) + 1
                self._mark_pool_submit_success(target_node_id)
                target_offset = (targets.index(target_node_id) + 1) % len(targets)
                accepted = True
                break
            if not accepted:
                failed.append(self._replay_failure_item(record, message=last_error))
        return failed

    def _submit_imap_entries_to_nodes(
        self,
        grouped: Dict[str, List[Tuple[int, Dict[str, object], pb2.TaskSubmitItem]]],
        *,
        node_order: Sequence[str],
        job_id: str,
        disabled_submit_nodes: Set[str],
        scheduler_failures: Dict[str, str],
        payload_buffer: _IndexedPayloadBuffer,
        inflight_by_node: Dict[str, int],
    ) -> int:
        submitted = 0
        for node_id in node_order:
            entries = grouped.get(node_id, [])
            if not entries:
                continue
            try:
                resp = self._submit_task_items_to_node(
                    node_id,
                    [item for _index, _payload, item in entries],
                    job_id=job_id,
                )
            except Exception as exc:
                disabled_submit_nodes.add(node_id)
                self._mark_pool_submit_failure(node_id, failure_kind=SUBMIT_FAILED, error=exc)
                scheduler_failures[node_id] = self._format_pool_submit_failure(node_id, exc)
                payload_buffer.requeue_front(entries)
                continue
            accepted_ids = {str(item.task_id) for item in resp.accepted if str(item.task_id).strip()}
            if not accepted_ids:
                self._mark_pool_submit_failure(
                    node_id,
                    failure_kind=SUBMIT_FAILED,
                    error=RuntimeError("submit returned no accepted task ids"),
                )
                payload_buffer.requeue_front(entries)
                continue
            self._mark_pool_submit_success(node_id)
            accepted_count = 0
            rejected_entries: List[Tuple[int, Dict[str, object], pb2.TaskSubmitItem]] = []
            for entry in entries:
                task_id = str(entry[2].task_id or "").strip()
                if task_id in accepted_ids:
                    accepted_count += 1
                    self._register_replay_record(
                        logical_index=int(entry[0]),
                        logical_key=int(entry[0]),
                        original_payload=dict(entry[1] or {}),
                        task_id=task_id,
                        node_id=node_id,
                        attempt=0,
                    )
                else:
                    rejected_entries.append(entry)
            if rejected_entries:
                payload_buffer.requeue_front(rejected_entries)
            inflight_by_node[node_id] = int(inflight_by_node.get(node_id, 0) or 0) + accepted_count
            submitted += accepted_count
        return submitted

    def _format_pool_submit_failure(self, node_id: str, exc: Exception) -> str:
        details = repr(exc)
        pool = self._pools.get(node_id)
        if pool is None:
            return details
        try:
            status = pool.get_status()
        except Exception as status_exc:
            return f"{details}; pool_status_error={status_exc!r}"
        fields = {
            "pool_id": str(getattr(pool, "pool_id", "") or ""),
            "pool_name": str(getattr(pool, "pool_name", "") or getattr(status, "pool_name", "") or ""),
            "status": str(getattr(status, "status", "") or getattr(pool, "status", "") or ""),
            "task_count": int(getattr(status, "task_count", 0) or 0),
            "worker_count": int(getattr(status, "worker_count", 0) or 0),
        }
        for key, value in fields.items():
            if value not in ("", 0):
                details += f"; {key}={value}"
        return details

    def _poll_imap_results_once(
        self,
        *,
        ordered_node_ids: Sequence[str],
        inflight_by_node: Dict[str, int],
        disabled_submit_nodes: Set[str],
        scheduler_failures: Dict[str, str],
        infra_failures_by_node: Dict[str, int],
        task_index_by_id: Dict[str, int],
        max_infra_retries: int,
        retry_backoff_ms: int,
        wait_ms: int = 0,
    ) -> Tuple[List[ExecutionItem], Dict[str, int]]:
        completed_items: List[ExecutionItem] = []
        freed_by_node: Dict[str, int] = {}
        blocking_node_id = ""
        for node_id in ordered_node_ids:
            if node_id in disabled_submit_nodes and int(inflight_by_node.get(node_id, 0) or 0) <= 0:
                continue
            blocking_node_id = str(node_id)
            if int(inflight_by_node.get(node_id, 0) or 0) > 0:
                break

        for node_id in ordered_node_ids:
            if node_id in disabled_submit_nodes and int(inflight_by_node.get(node_id, 0) or 0) <= 0:
                continue
            pull_limit = max(1, int(inflight_by_node.get(node_id, 0) or 1))
            per_pull_wait_ms = max(0, int(wait_ms or 0)) if str(node_id) == blocking_node_id else 0
            try:
                resp = self._pools[node_id].pull_results(limit=pull_limit, wait_ms=per_pull_wait_ms, cursor="")
            except Exception as exc:
                disabled_submit_nodes.add(node_id)
                records, _orphan_task_ids = self._mark_taskpool_node_lost(node_id, error=exc)
                inflight_by_node[node_id] = 0
                scheduler_failures[node_id] = repr(exc)
                completed_items.extend(
                    self._retry_replay_records(
                        records,
                        reason=repr(exc),
                        disabled_submit_nodes=disabled_submit_nodes,
                        scheduler_failures=scheduler_failures,
                        inflight_by_node=inflight_by_node,
                        task_index_by_id=task_index_by_id,
                        max_infra_retries=max_infra_retries,
                        retry_backoff_ms=retry_backoff_ms,
                    )
                )
                continue
            if not resp.results:
                continue
            for result in resp.results:
                normalized = str(result.task_id or "").strip()
                if not self._is_pending_task_id(normalized):
                    continue
                replay_record = self._take_replay_record(normalized)
                self._mark_result_consumed(result.task_id)
                inflight_by_node[node_id] = max(0, int(inflight_by_node.get(node_id, 0) or 0) - 1)
                freed_by_node[node_id] = int(freed_by_node.get(node_id, 0) or 0) + 1
                if int(result.status) == int(pb2.TASK_STATUS_FAILED_INFRA):
                    infra_failures_by_node[node_id] = int(infra_failures_by_node.get(node_id, 0) or 0) + 1
                    self._mark_pool_submit_failure(
                        node_id,
                        failure_kind=REMOTE_INFRA_FAILED,
                        error=RuntimeError(str(result.error.message or "remote infra failure")),
                    )
                    if int(infra_failures_by_node.get(node_id, 0) or 0) >= 2:
                        disabled_submit_nodes.add(node_id)
                    if replay_record is not None:
                        completed_items.extend(
                            self._retry_replay_records(
                                [replay_record],
                                reason=str(result.error.message or "remote infra failure"),
                                disabled_submit_nodes=disabled_submit_nodes,
                                scheduler_failures=scheduler_failures,
                                inflight_by_node=inflight_by_node,
                                task_index_by_id=task_index_by_id,
                                max_infra_retries=max_infra_retries,
                                retry_backoff_ms=retry_backoff_ms,
                            )
                        )
                        continue
                elif int(result.status) == int(pb2.TASK_STATUS_SUCCEEDED):
                    infra_failures_by_node.pop(node_id, None)
                    self._mark_pool_submit_success(node_id)
                item = self._task_result_to_item(node_id, result)
                index = int(task_index_by_id.get(normalized, -1))
                completed_items.append(self._item_with_index(item, index=index, key=index if index >= 0 else normalized))
        return completed_items, freed_by_node

    def _fill_imap_from_quota(
        self,
        available_by_node: Dict[str, int],
        *,
        node_order: Sequence[str],
        max_pending: int,
        profile,
        inflight_by_node: Dict[str, int],
        disabled_submit_nodes: Set[str],
        infra_failures_by_node: Dict[str, int],
        poll_start_idx: int,
        payload_buffer: _IndexedPayloadBuffer,
        task_index_by_id: Dict[str, int],
        scheduler_failures: Dict[str, str],
    ) -> int:
        available_global = max(0, int(max_pending or 0) - sum(inflight_by_node.values()))
        if available_global <= 0:
            return 0
        capped_by_node = {
            node_id: max(0, int(available_by_node.get(node_id, 0) or 0))
            for node_id in node_order
            if node_id not in disabled_submit_nodes
        }
        targets = self._plan_limited_pool_node_targets(
            capped_by_node,
            node_order=node_order,
            max_new_tasks=available_global,
            profile=profile,
            inflight_by_node=inflight_by_node,
            disabled_submit_nodes=disabled_submit_nodes,
            infra_failures_by_node=infra_failures_by_node,
            round_robin_start=poll_start_idx,
        )
        if not targets:
            return 0
        grouped: Dict[str, List[Tuple[int, Dict[str, object], pb2.TaskSubmitItem]]] = {}
        for node_id in targets:
            indexed_payload = payload_buffer.next()
            if indexed_payload is None:
                break
            index, payload = indexed_payload
            item = self._build_task_submit_item_for_node(
                node_id=node_id,
                payload=payload,
                timeout_hint_sec=0,
                priority=1,
            )
            task_index_by_id[str(item.task_id)] = int(index)
            grouped.setdefault(node_id, []).append((int(index), payload, item))
        return self._submit_imap_entries_to_nodes(
            grouped,
            node_order=node_order,
            job_id=self.job_id,
            disabled_submit_nodes=disabled_submit_nodes,
            scheduler_failures=scheduler_failures,
            payload_buffer=payload_buffer,
            inflight_by_node=inflight_by_node,
        )

    def imap_unordered(
        self,
        payloads: Iterable[Dict[str, object]],
        *,
        task_method: str = "",
        strategy: str = "taskpool_default",
        max_in_flight: Optional[int] = None,
        timeout_sec: float = 30.0,
        return_items: bool = False,
        receive_batch: int = 1,
        submit_timeout_sec: Optional[float] = None,
        result_timeout_sec: Optional[float] = None,
        server_wait_ms: Optional[int] = None,
        wait_ms: int = 500,
        raise_on_error: bool = True,
        max_infra_retries: int = 1,
        retry_backoff_ms: int = 0,
        node_window_factor: float = 2.0,
        progress: ProgressOption = False,
        progress_interval_sec: float = 2.0,
    ) -> Iterator[Union[Tuple[int, Any], ExecutionItem]]:
        if self._closed:
            raise RuntimeError("task pool session is closed")
        self._enter_exclusive_mode("imap_unordered", require_clean=True)
        completed = 0
        succeeded = 0
        failed = 0
        last_error = ""
        submitted = 0
        try:
            total = len(payloads)  # type: ignore[arg-type]
        except Exception:
            total = 0
        reporter = ProgressReporter(
            progress,
            label=f"taskpool.{str(task_method or self._task_method).strip() or self._task_method}",
            total=max(0, int(total or 0)),
            interval_sec=progress_interval_sec,
        )
        try:
            self._ensure_method(str(task_method or self._task_method).strip() or self._task_method)
            max_pending = self._resolve_max_in_flight(max_in_flight)
            max_receive = max(1, int(receive_batch or 1))
            submit_timeout_sec = float(submit_timeout_sec if submit_timeout_sec is not None else timeout_sec)
            result_timeout_sec = float(result_timeout_sec if result_timeout_sec is not None else timeout_sec)
            wait_ms = int(server_wait_ms if server_wait_ms is not None else (wait_ms or 500))
            max_infra_retries = max(0, int(max_infra_retries or 0))
            retry_backoff_ms = max(0, int(retry_backoff_ms or 0))
            _node_window_factor = float(node_window_factor or 2.0)
            profile = resolve_taskpool_strategy(strategy)
            payload_buffer = _IndexedPayloadBuffer(payloads)
            ready_items: "deque[ExecutionItem]" = deque()
            task_index_by_id: Dict[str, int] = {}
            node_ids = self._available_pool_node_ids()
            if not node_ids:
                raise RuntimeError("task pool has no active node pools")
            inflight_by_node = {node_id: 0 for node_id in node_ids}
            disabled_submit_nodes: set[str] = set()
            scheduler_failures: Dict[str, str] = {}
            infra_failures_by_node: Dict[str, int] = {}
            poll_start_idx = 0
            wait_deadline = time.time() + max(0.1, float(result_timeout_sec))
            cancelled_for_error = False
            reporter.emit(
                phase="running",
                completed=completed,
                succeeded=succeeded,
                failed=failed,
                inflight=0,
                submitted=submitted,
                force=True,
            )

            initial_quota = {node_id: max_pending for node_id in node_ids}
            self._fill_imap_from_quota(
                initial_quota,
                node_order=node_ids,
                max_pending=max_pending,
                profile=profile,
                inflight_by_node=inflight_by_node,
                disabled_submit_nodes=disabled_submit_nodes,
                infra_failures_by_node=infra_failures_by_node,
                poll_start_idx=poll_start_idx,
                payload_buffer=payload_buffer,
                task_index_by_id=task_index_by_id,
                scheduler_failures=scheduler_failures,
            )
            submitted = payload_buffer.submitted_count
            reporter.emit(
                phase="running",
                completed=completed,
                succeeded=succeeded,
                failed=failed,
                inflight=sum(inflight_by_node.values()),
                submitted=submitted,
            )
            if self._pending_result_count() <= 0 and not payload_buffer.has_retry and payload_buffer.exhausted:
                if submitted <= 0:
                    logger.warning(
                        "task pool imap_unordered exited with zero submitted tasks "
                        "pool_name=%s job_id=%s routes=%s",
                        str(getattr(next(iter(self._pools.values()), None), "pool_name", "") or ""),
                        self.job_id,
                        self.route_summary(),
                    )
                reporter.done(completed=completed, succeeded=succeeded, failed=failed, submitted=submitted)
                return

            while True:
                yielded = 0
                while ready_items and yielded < max_receive:
                    item = ready_items.popleft()
                    completed += 1
                    if not item.ok:
                        failed += 1
                        last_error = str(item.error_message or item.error_type or "")
                        if raise_on_error:
                            if not cancelled_for_error:
                                with contextlib.suppress(Exception):
                                    self.cancel_job(reason="imap_unordered task failure", job_id=self.job_id)
                                cancelled_for_error = True
                            reporter.emit(
                                phase="failed",
                                completed=completed,
                                succeeded=succeeded,
                                failed=failed,
                                inflight=sum(inflight_by_node.values()),
                                submitted=submitted,
                                last_error=last_error,
                                force=True,
                            )
                            raise RuntimeError(item.error_message or f"task failed: {item.task_id}")
                        reporter.emit(
                            phase="running",
                            completed=completed,
                            succeeded=succeeded,
                            failed=failed,
                            inflight=sum(inflight_by_node.values()),
                            submitted=submitted,
                            last_error=last_error,
                        )
                        yield item if return_items else (item.index, None)
                    else:
                        succeeded += 1
                        reporter.emit(
                            phase="running",
                            completed=completed,
                            succeeded=succeeded,
                            failed=failed,
                            inflight=sum(inflight_by_node.values()),
                            submitted=submitted,
                            last_error=last_error,
                        )
                        yield item if return_items else (item.index, item.data)
                    yielded += 1
                if yielded > 0:
                    continue

                if payload_buffer.exhausted and not payload_buffer.has_retry and self._pending_result_count() <= 0:
                    reporter.done(completed=completed, succeeded=succeeded, failed=failed, submitted=submitted, last_error=last_error)
                    return

                if self._pending_result_count() <= 0:
                    idle_quota = {node_id: max_pending for node_id in node_ids}
                    submitted_now = self._fill_imap_from_quota(
                        idle_quota,
                        node_order=node_ids,
                        max_pending=max_pending,
                        profile=profile,
                        inflight_by_node=inflight_by_node,
                        disabled_submit_nodes=disabled_submit_nodes,
                        infra_failures_by_node=infra_failures_by_node,
                        poll_start_idx=poll_start_idx,
                        payload_buffer=payload_buffer,
                        task_index_by_id=task_index_by_id,
                        scheduler_failures=scheduler_failures,
                    )
                    if submitted_now > 0:
                        submitted = payload_buffer.submitted_count
                        reporter.emit(
                            phase="running",
                            completed=completed,
                            succeeded=succeeded,
                            failed=failed,
                            inflight=sum(inflight_by_node.values()),
                            submitted=submitted,
                            last_error=last_error,
                        )
                        wait_deadline = time.time() + max(0.1, float(result_timeout_sec))
                        continue
                    if not payload_buffer.has_retry and not payload_buffer.exhausted:
                        next_payload = payload_buffer.next()
                        if next_payload is None:
                            continue
                        payload_buffer.requeue_front([(next_payload[0], next_payload[1], None)])
                    if payload_buffer.has_retry or not payload_buffer.exhausted:
                        failure_suffix = f"; failures={scheduler_failures}" if scheduler_failures else ""
                        last_error = f"imap_unordered could not submit tasks to any active task pool node{failure_suffix}"
                        reporter.emit(
                            phase="failed",
                            completed=completed,
                            succeeded=succeeded,
                            failed=failed,
                            inflight=sum(inflight_by_node.values()),
                            submitted=submitted,
                            last_error=last_error,
                            force=True,
                        )
                        raise RuntimeError(last_error)
                    reporter.done(completed=completed, succeeded=succeeded, failed=failed, submitted=submitted, last_error=last_error)
                    return

                ordered_node_ids = node_ids[poll_start_idx:] + node_ids[:poll_start_idx]
                poll_start_idx = (poll_start_idx + 1) % len(node_ids)
                completed_items, freed_by_node = self._poll_imap_results_once(
                    ordered_node_ids=ordered_node_ids,
                    inflight_by_node=inflight_by_node,
                    disabled_submit_nodes=disabled_submit_nodes,
                    scheduler_failures=scheduler_failures,
                    infra_failures_by_node=infra_failures_by_node,
                    task_index_by_id=task_index_by_id,
                    max_infra_retries=max_infra_retries,
                    retry_backoff_ms=retry_backoff_ms,
                    wait_ms=wait_ms,
                )

                if completed_items:
                    wait_deadline = time.time() + max(0.1, float(result_timeout_sec))
                    ready_items.extend(completed_items)
                    if not (raise_on_error and any(not item.ok for item in completed_items)):
                        freed_total = max(0, sum(int(value or 0) for value in freed_by_node.values()))
                        if freed_total > 0:
                            refill_quota = {node_id: freed_total for node_id in node_ids}
                            self._fill_imap_from_quota(
                                refill_quota,
                                node_order=node_ids,
                                max_pending=max_pending,
                                profile=profile,
                                inflight_by_node=inflight_by_node,
                                disabled_submit_nodes=disabled_submit_nodes,
                                infra_failures_by_node=infra_failures_by_node,
                                poll_start_idx=poll_start_idx,
                                payload_buffer=payload_buffer,
                                task_index_by_id=task_index_by_id,
                                scheduler_failures=scheduler_failures,
                            )
                            submitted = payload_buffer.submitted_count
                            reporter.emit(
                                phase="running",
                                completed=completed,
                                succeeded=succeeded,
                                failed=failed,
                                inflight=sum(inflight_by_node.values()),
                                submitted=submitted,
                                last_error=last_error,
                            )
                    continue

                if time.time() >= wait_deadline:
                    pending_by_node = self._pending_result_count_by_node()
                    last_error = (
                        f"imap_unordered did not receive results before timeout; pending_task_ids={self._pending_result_count()}"
                    )
                    if pending_by_node:
                        last_error += f"; pending_by_node={pending_by_node}"
                    stalled_node_ids = [
                        node_id
                        for node_id in node_ids
                        if int(inflight_by_node.get(node_id, 0) or 0) > 0 and node_id not in disabled_submit_nodes
                    ]
                    retry_candidate_ids = [
                        node_id
                        for node_id in node_ids
                        if node_id not in disabled_submit_nodes and node_id not in stalled_node_ids
                    ]
                    if not retry_candidate_ids:
                        retry_candidate_ids = [
                            node_id
                            for node_id in node_ids
                            if node_id not in disabled_submit_nodes and int(inflight_by_node.get(node_id, 0) or 0) > 0
                        ]
                    if max_infra_retries > 0 and stalled_node_ids and retry_candidate_ids:
                        replay_records: List[_TaskReplayRecord] = []
                        for stalled_node_id in stalled_node_ids:
                            records, _orphan_task_ids = self._take_pending_replay_records_for_node(stalled_node_id)
                            if not records:
                                continue
                            inflight_by_node[stalled_node_id] = 0
                            scheduler_failures[stalled_node_id] = last_error
                            replay_records.extend(records)
                        if replay_records:
                            exhausted_timeout_records = [
                                record for record in replay_records if int(record.timeout_retry_count or 0) >= 1
                            ]
                            if exhausted_timeout_records:
                                last_error += f"; timeout_retry_exhausted={len(exhausted_timeout_records)}"
                                reporter.emit(
                                    phase="failed",
                                    completed=completed,
                                    succeeded=succeeded,
                                    failed=failed,
                                    inflight=sum(inflight_by_node.values()),
                                    submitted=submitted,
                                    last_error=last_error,
                                    force=True,
                                )
                                raise TimeoutError(last_error)
                            replay_records = [
                                record for record in replay_records if int(record.timeout_retry_count or 0) < 1
                            ]
                            retry_failed_items = self._retry_timeout_replay_records_evenly(
                                replay_records,
                                reason=last_error,
                                target_node_ids=retry_candidate_ids,
                                disabled_submit_nodes=disabled_submit_nodes,
                                scheduler_failures=scheduler_failures,
                                inflight_by_node=inflight_by_node,
                                task_index_by_id=task_index_by_id,
                            )
                            if retry_failed_items:
                                ready_items.extend(retry_failed_items)
                            submitted = payload_buffer.submitted_count
                            wait_deadline = time.time() + max(0.1, float(result_timeout_sec))
                            reporter.emit(
                                phase="running",
                                completed=completed,
                                succeeded=succeeded,
                                failed=failed,
                                inflight=sum(inflight_by_node.values()),
                                submitted=submitted,
                                last_error=f"{last_error}; replayed_timed_out_tasks={len(replay_records)}",
                            )
                            continue
                    reporter.emit(
                        phase="failed",
                        completed=completed,
                        succeeded=succeeded,
                        failed=failed,
                        inflight=sum(inflight_by_node.values()),
                        submitted=submitted,
                        last_error=last_error,
                        force=True,
                    )
                    raise TimeoutError(last_error)
                time.sleep(max(0.01, min(0.1, wait_ms / 1000.0 if wait_ms > 0 else 0.02)))
        except GeneratorExit:
            raise
        except Exception as exc:
            if not last_error:
                last_error = str(exc)
                reporter.emit(
                    phase="failed",
                    completed=completed,
                    succeeded=succeeded,
                    failed=failed,
                    submitted=submitted,
                    last_error=last_error,
                    force=True,
                )
            raise
        finally:
            self._exit_exclusive_mode("imap_unordered")

    def unordered(
        self,
        payloads: Iterable[Dict[str, object]],
        *,
        task_method: str = "",
        strategy: str = "taskpool_default",
        max_in_flight: Optional[int] = None,
        timeout_sec: float = 30.0,
        return_items: bool = False,
        progress: ProgressOption = False,
        progress_interval_sec: float = 2.0,
    ) -> Iterator[Union[Tuple[int, Any], ExecutionItem]]:
        """Yield ``(index, result_or_none)`` in completion order for a submitted batch."""
        progress_kwargs = {}
        if progress:
            progress_kwargs["progress"] = progress
            progress_kwargs["progress_interval_sec"] = progress_interval_sec
        for item in self.iter_items(
            payloads,
            task_method=task_method,
            strategy=strategy,
            max_in_flight=max_in_flight,
            timeout_sec=timeout_sec,
            **progress_kwargs,
        ):
            yield item if return_items else (item.index, item.result if item.ok else None)

    async def aunordered(
        self,
        payloads: Iterable[Dict[str, object]],
        *,
        task_method: str = "",
        strategy: str = "taskpool_default",
        max_in_flight: Optional[int] = None,
        timeout_sec: float = 30.0,
        return_items: bool = False,
        progress: ProgressOption = False,
        progress_interval_sec: float = 2.0,
    ) -> AsyncIterator[Union[Tuple[int, Any], ExecutionItem]]:
        """Async counterpart of :meth:`unordered` with the same return shape."""
        progress_kwargs = {}
        if progress:
            progress_kwargs["progress"] = progress
            progress_kwargs["progress_interval_sec"] = progress_interval_sec
        async for item in self.aiter_items(
            payloads,
            task_method=task_method,
            strategy=strategy,
            max_in_flight=max_in_flight,
            timeout_sec=timeout_sec,
            **progress_kwargs,
        ):
            yield item if return_items else (item.index, item.result if item.ok else None)

    def consume_unordered(
        self,
        payloads: Iterable[Dict[str, object]],
        *,
        handle: Callable[[int, Any], Any],
        task_method: str = "",
        strategy: str = "taskpool_default",
        max_in_flight: Optional[int] = None,
        timeout_sec: float = 30.0,
        receive_batch: int = 1,
        submit_timeout_sec: float = 60.0,
        result_timeout_sec: float = 30.0,
        wait_ms: int = 500,
        raise_on_error: bool = True,
        node_window_factor: float = 2.0,
        progress: ProgressOption = False,
        progress_interval_sec: float = 2.0,
    ) -> int:
        if not callable(handle):
            raise TypeError("handle must be callable")
        processed = 0
        progress_kwargs = {}
        if progress:
            progress_kwargs["progress"] = progress
            progress_kwargs["progress_interval_sec"] = progress_interval_sec
        for task_id, result in self.imap_unordered(
            payloads,
            task_method=task_method,
            strategy=strategy,
            max_in_flight=max_in_flight,
            receive_batch=receive_batch,
            submit_timeout_sec=submit_timeout_sec,
            result_timeout_sec=result_timeout_sec,
            wait_ms=wait_ms,
            raise_on_error=raise_on_error,
            node_window_factor=node_window_factor,
            return_items=False,
            **progress_kwargs,
        ):
            index: Union[int, str] = task_id
            if isinstance(task_id, str) and task_id.rsplit("-", 1)[-1].isdigit():
                index = max(0, int(task_id.rsplit("-", 1)[-1]) - 1)
            handle(index, result)
            processed += 1
        return processed

    def map(
        self,
        values: Iterable[Any],
        *,
        arg_name: str = "value",
        task_method: str = "",
        strategy: str = "taskpool_default",
        max_in_flight: Optional[int] = None,
        timeout_sec: float = 30.0,
        progress: ProgressOption = False,
        progress_interval_sec: float = 2.0,
        **shared_kwargs,
    ) -> Sequence[Any]:
        normalized_arg = str(arg_name or "value").strip() or "value"
        shared = dict(shared_kwargs)
        if not is_progress_option(progress):
            shared = {"progress": progress, **shared}
            progress = False
        try:
            progress_total = len(values)  # type: ignore[arg-type]
        except Exception:
            progress_total = None
        payloads = ({normalized_arg: value, **shared} for value in values)
        progress_kwargs = {}
        if progress:
            progress_kwargs["progress"] = progress
            progress_kwargs["progress_interval_sec"] = progress_interval_sec
            if progress_total is not None:
                payloads = _SizedPayloadIterable(payloads, progress_total)
        items = self.collect_items(
            payloads,
            task_method=task_method,
            strategy=strategy,
            max_in_flight=self._resolve_max_in_flight(max_in_flight),
            timeout_sec=timeout_sec,
            **progress_kwargs,
        )
        return [item.result if item.ok else None for item in items]

    def map_values(
        self,
        values: Iterable[Any],
        *,
        arg_name: str = "value",
        task_method: str = "",
        strategy: str = "taskpool_default",
        max_in_flight: Optional[int] = None,
        timeout_sec: float = 30.0,
        progress: ProgressOption = False,
        progress_interval_sec: float = 2.0,
        **shared_kwargs,
    ) -> Sequence[Any]:
        """Map a sequence of local values to remote task calls.

        This is the explicit alias for ``map(...)``. It does not accept a local
        Python callable like the built-in ``map``; each value is sent as
        ``{arg_name: value}`` to the remote task method.
        """
        progress_kwargs = {}
        if progress:
            progress_kwargs["progress"] = progress
            progress_kwargs["progress_interval_sec"] = progress_interval_sec
        return self.map(
            values,
            arg_name=arg_name,
            task_method=task_method,
            strategy=strategy,
            max_in_flight=max_in_flight,
            timeout_sec=timeout_sec,
            **progress_kwargs,
            **shared_kwargs,
        )

    async def amap(
        self,
        values: Iterable[Any],
        *,
        arg_name: str = "value",
        task_method: str = "",
        strategy: str = "taskpool_default",
        max_in_flight: Optional[int] = None,
        timeout_sec: float = 30.0,
        progress: ProgressOption = False,
        progress_interval_sec: float = 2.0,
        **shared_kwargs,
    ) -> Sequence[Any]:
        progress_kwargs = {}
        if progress:
            progress_kwargs["progress"] = progress
            progress_kwargs["progress_interval_sec"] = progress_interval_sec
        return await asyncio.to_thread(
            lambda: self.map(
                values,
                arg_name=arg_name,
                task_method=task_method,
                strategy=strategy,
                max_in_flight=max_in_flight,
                timeout_sec=timeout_sec,
                **progress_kwargs,
                **shared_kwargs,
            )
        )

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

    def execution_identity(self) -> SessionIdentity:
        first = next(iter(self._pools.values()))
        return first.identity()

    def execution_binding(self) -> SessionBinding:
        first = next(iter(self._pools.values()))
        return first.binding()

    def execution_snapshot(self):
        return super().snapshot()

    def execution_status(self) -> ExecutionSessionStatus:
        return super().status()

    def status(self) -> ExecutionSessionStatus:
        return self.execution_status()

    def is_alive(self) -> bool:
        return (not self._closed) and (not self.failed) and any(
            node_id in self._active_nodes for node_id in self._pools
        )

    def put_data(
        self,
        data: Any,
        *,
        format: str = "",
        object_format: str = "",
        chunk_size: int = OBJECT_CHUNK_SIZE_BYTES,
        serialization_mode: str = "",
    ) -> DataRef:
        """Upload data to the task-pool object store and return a DataRef.

        ``object_format`` is the preferred explicit name for the object-store
        format hint. ``format`` remains accepted for compatibility.
        """
        if self._closed:
            raise RuntimeError("task pool session is closed")
        effective_format = str(object_format or format or "")
        pools_snapshot = list(self._pools.values())
        active_clients = [pool._client for pool in pools_snapshot]  # noqa: SLF001
        if self._is_local_session():
            effective_serialization_mode = LOCAL_IPC_SERIALIZATION_MODE
        else:
            effective_serialization_mode = resolve_effective_serialization_mode(
                request_mode=serialization_mode,
                context="object_upload",
                frozen_mode=self._serialization_mode,
            )
        return _put_data_via_clients(
            active_clients,
            data,
            format=effective_format,
            chunk_size=chunk_size,
            serialization_mode=effective_serialization_mode,
        )

    def put_dataframe(
        self,
        dataframe: Any,
        *,
        chunk_size: int = OBJECT_CHUNK_SIZE_BYTES,
        serialization_mode: str = "",
    ) -> DataRef:
        return self.put_data(dataframe, format="parquet", chunk_size=chunk_size, serialization_mode=serialization_mode)

    def put_ndarray(
        self,
        array: Any,
        *,
        chunk_size: int = OBJECT_CHUNK_SIZE_BYTES,
        serialization_mode: str = "",
    ) -> DataRef:
        return self.put_data(array, format="npy", chunk_size=chunk_size, serialization_mode=serialization_mode)

    def put_json(
        self,
        value: Any,
        *,
        chunk_size: int = OBJECT_CHUNK_SIZE_BYTES,
        serialization_mode: str = "",
    ) -> DataRef:
        return self.put_data(value, format="json", chunk_size=chunk_size, serialization_mode=serialization_mode)

    def close(self, reason: str = "task pool session close") -> None:
        if self._closed:
            return
        self._closed = True
        self._stop_keepalive()
        close_reason = str(reason or "task pool session close")
        for pool in self._pools.values():
            _close_task_pool_replica(pool, reason=close_reason)
            with contextlib.suppress(Exception):
                pool._client.close()  # noqa: SLF001

    def __enter__(self) -> "_TaskPoolSessionBase":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def __getattr__(self, name: str):
        if name.startswith("_"):
            raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")
        return _TaskPoolCallProxy(session=self, method_name=self._ensure_method(name))

    def call_sync(self, method: str, **kwargs) -> Any:
        """Synchronously call the task-pool entry method."""
        normalized = self._ensure_method(method)
        return getattr(self, normalized).sync(**kwargs)

    async def call(self, method: str, **kwargs) -> Any:
        """Asynchronously call the task-pool entry method.

        Use ``call_sync(...)`` for the synchronous variant.
        """
        normalized = self._ensure_method(method)
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, lambda: getattr(self, normalized).sync(**kwargs))

    async def call_async(self, method: str, **kwargs) -> Any:
        """Explicit alias for ``call(...)``."""
        return await self.call(method, **kwargs)

    def __repr__(self) -> str:
        effective_policy_text = ""
        if self.effective_policy is not None:
            effective_policy_text = (
                f" effective_policy={self.effective_policy.policy_id}@v{self.effective_policy.version}"
            )
        return (
            f"<{type(self).__name__} methods={self.methods} "
            f"nodes={len(self.node_ids)} serialization_mode={self._serialization_mode}"
            f"{effective_policy_text}>"
        )


def _build_task_pool_from_infocenter(
    cls,
    *,
    infocenter_target: str,
    job_id: str = "",
    source: Any = None,
    owner_client_id: Optional[str] = None,
    pool_name: Optional[str] = None,
    artifact: Optional[Any] = None,
    deps: Optional[Any] = None,
    runtime: str = "py3",
    entry_module: Any = "",
    entry_callable: Any = "run",
    package_format: str = "",
    resource_paths: Optional[Sequence[Any]] = None,
    managed_global_names: Optional[Sequence[str]] = None,
    initial_globals: Optional[Dict[str, object]] = None,
    worker_count: int = 1,
    heartbeat_timeout_sec: int = 0,
    idle_ttl_sec: int = 0,
    chunk_size: int = OBJECT_CHUNK_SIZE_BYTES,
    healthy_only: bool = True,
    tags: Optional[Sequence[str]] = None,
    node_ids: Optional[Sequence[str]] = None,
    node_instance_ids: Optional[Sequence[str]] = None,
    node_count: int = 0,
    node_limit: int = 100,
    timeout_sec: float = 10.0,
    serialization_mode: str = "",
    policy_id: str = "",
    api_token: str = "",
) -> "TaskPool":
    initial_globals_values, effective_managed_global_names = normalize_initial_globals(initial_globals, managed_global_names)
    prepared_artifact = prepare_deployment_artifact(
        consumer_kind="task",
        source=source,
        artifact=artifact,
        deps=deps,
        runtime=runtime,
        entry_module=entry_module,
        entry_callable=entry_callable,
        package_format=package_format,
        managed_global_names=effective_managed_global_names,
        resource_paths=resource_paths,
    )
    effective_blob = prepared_artifact.blob
    runtime = prepared_artifact.runtime
    entry_module = prepared_artifact.entry_module
    entry_callable = prepared_artifact.entry_callable
    effective_package_format = prepared_artifact.package_format
    dependency_allowlist = list(prepared_artifact.dependency_allowlist)
    managed_global_names = list(prepared_artifact.managed_global_names)
    effective_heartbeat_timeout_sec = get_taskpool_heartbeat_timeout_sec(heartbeat_timeout_sec)
    create_rpc_timeout_sec = _taskpool_create_rpc_timeout_sec(timeout_sec)

    effective_owner = str(owner_client_id or f"client-{_get_local_ip()}").strip()
    requested_count = max(0, int(node_count or 0))
    requested_node_ids = [str(node_id).strip() for node_id in list(node_ids or ()) if str(node_id).strip()]
    requested_node_instance_ids = [str(node_id).strip() for node_id in list(node_instance_ids or ()) if str(node_id).strip()]
    compensation_target_count = requested_count or len(requested_node_instance_ids) or len(requested_node_ids)
    explicit_node_selection = bool(requested_node_ids or requested_node_instance_ids)
    pinned_instance_selection = bool(requested_node_instance_ids)
    fetch_limit = requested_count if (requested_count > 0 and explicit_node_selection) else node_limit
    create_failures: Dict[str, str] = {}

    def _select_candidate_nodes() -> List[InfoCenterNode]:
        def _select_once() -> List[InfoCenterNode]:
            with _infocenter_client(infocenter_target, timeout_sec=timeout_sec) as infocenter:
                return list(
                    infocenter.select_task_nodes(
                        healthy_only=healthy_only,
                        tags=tags,
                        node_ids=requested_node_ids,
                        node_instance_ids=requested_node_instance_ids,
                        node_count=fetch_limit,
                        limit=node_limit,
                        require_credit=False,
                        preferred_runtime_key="",
                        runtime=runtime,
                    )
                )

        return _retry_infocenter_request(
            _select_once,
            timeout_sec=max(0.5, float(timeout_sec or 0.0)),
            target=infocenter_target,
            action="task pool node discovery",
        )

    def _current_healthy_node_keys() -> set[str]:
        try:
            with _infocenter_client(infocenter_target, timeout_sec=max(0.5, min(5.0, float(timeout_sec or 0.0)))) as infocenter:
                return {
                    _node_instance_key_from_node(node)
                    for node in infocenter.list_nodes(healthy_only=True, tags=tags, limit=node_limit)
                    if _node_instance_key_from_node(node)
                }
        except Exception as exc:
            logger.info(
                "task pool create recovery could not refresh healthy node snapshot pool_name=%s err=%r",
                pool_name or "",
                exc,
            )
            return set()

    selected_nodes = _select_candidate_nodes()
    if not selected_nodes:
        raise RuntimeError("no task pool nodes selected from InfoCenter")
    all_selected_nodes: List[InfoCenterNode] = list(selected_nodes)
    required_success_nodes = requested_count if requested_count > 0 else len(selected_nodes)
    desired_nodes = selected_nodes[:required_success_nodes]
    fallback_nodes = selected_nodes[required_success_nodes:] if not explicit_node_selection else []
    effective_pool_name = str(pool_name or f"task-pool-{uuid.uuid4().hex[:10]}").strip()
    create_request_namespace = uuid.uuid4().hex
    create_request_ids: Dict[str, str] = {}
    effective_policy = resolve_effective_policy(
        get_policy_profile(policy_id or get_default_policy_id_for_binding("taskpool_default")),
        requested_mode=serialization_mode,
        context="taskpool_session",
    )
    effective_api_token = _resolve_owner_api_token(api_token)

    def _create_pool_on_node(node: InfoCenterNode) -> Tuple[InfoCenterNode, NativeTaskPoolClient]:
        target = _node_control_target_for_node(node)
        client = _new_node_control_client(target, timeout_sec=create_rpc_timeout_sec)
        node_key = _node_instance_key_from_node(node)
        create_request_id = create_request_ids.setdefault(
            node_key,
            f"taskpool-create:{effective_owner}:{effective_pool_name}:{create_request_namespace}:{node_key}",
        )
        node_worker_count = max(1, int(worker_count or 1))
        node_available = int(getattr(node, "task_pool_worker_available", 0) or 0)
        if node_available > 0:
            node_worker_count = max(1, min(node_worker_count, node_available))
        try:
            pool = client.create_task_pool_from_bytes(
                owner_client_id=effective_owner,
                pool_name=effective_pool_name,
                blob=effective_blob,
                runtime=runtime,
                entry_module=entry_module,
                entry_callable=entry_callable,
                package_format=effective_package_format,
                deps=prepared_artifact.dependency_policy,
                managed_global_names=managed_global_names,
                initial_globals=initial_globals_values,
                worker_count=node_worker_count,
                heartbeat_timeout_sec=effective_heartbeat_timeout_sec,
                idle_ttl_sec=idle_ttl_sec,
                chunk_size=chunk_size,
                api_token=effective_api_token,
                expected_node_instance_id=_node_instance_key_from_node(node),
                create_request_id=create_request_id,
            )
        except Exception:
            with contextlib.suppress(Exception):
                client.close()
            raise
        return node, pool

    created: List[Tuple[InfoCenterNode, NativeTaskPoolClient]] = []

    def _record_create_results(nodes_to_try: Sequence[InfoCenterNode]) -> None:
        dispatch_results = dispatch_create_requests(
            nodes_to_try,
            create_one=_create_pool_on_node,
            thread_name_prefix="taskpool-create",
            describe_error=lambda node, exc: repr(exc),
        )
        for item in dispatch_results:
            node_key = _node_instance_key_from_node(item.node)
            if item.error_message:
                create_failures[node_key] = item.error_message
                if _is_node_identity_mismatch_error(item.error_message):
                    _mark_infocenter_node_lost_on_identity_mismatch(
                        infocenter_factory=_infocenter_client,
                        infocenter_target=infocenter_target,
                        timeout_sec=timeout_sec,
                        node_instance_id=node_key,
                        error_message=item.error_message,
                        reason_prefix="task pool create identity mismatch",
                    )
                logger.warning(
                    "task pool replica create failed pool_name=%s node_id=%s node_instance_id=%s "
                    "control_addr=%s category=%s err=%s",
                    effective_pool_name,
                    getattr(item.node, "node_id", ""),
                    node_key,
                    getattr(item.node, "control_addr", ""),
                    classify_error(item.error_message, resource_kind="task_pool").value,
                    item.error_message,
                )
                continue
            if item.created is not None:
                create_failures.pop(node_key, None)
                created.append(item.created)

    _record_create_results(desired_nodes)
    if len(created) < required_success_nodes and fallback_nodes:
        _emit_taskpool_notice(
            "open retrying alternate nodes after create failure "
            f"pool_name={effective_pool_name} success={len(created)} "
            f"required={required_success_nodes} "
            f"fallback_candidates={[_node_instance_key_from_node(node) for node in fallback_nodes]} "
            f"selected={_summarize_discovered_nodes(selected_nodes)}"
        )
        for fallback_node in fallback_nodes:
            if len(created) >= required_success_nodes:
                break
            _record_create_results([fallback_node])
    if (
        len(created) < required_success_nodes
        and not pinned_instance_selection
        and should_retry_replica_create_failures(
            create_failures,
            success=len(created),
            required=required_success_nodes,
            resource_kind="task_pool",
        )
    ):
        def _should_continue_create_recovery() -> bool:
            return should_retry_replica_create_failures(
                create_failures,
                success=len(created),
                required=required_success_nodes,
                resource_kind="task_pool",
            )

        def _attempt_create_recovery(retry_attempt: int) -> None:
            try:
                retry_nodes = _select_candidate_nodes()
            except Exception as exc:
                create_failures[f"infocenter-retry-{retry_attempt}"] = repr(exc)
                return
            if not retry_nodes:
                return
            tried_node_keys = set(create_failures.keys()) | {
                _node_instance_key_from_node(node) for node, _pool in created
            }
            fresh_retry_nodes = [
                node
                for node in retry_nodes
                if _node_instance_key_from_node(node) not in tried_node_keys
            ]
            if fresh_retry_nodes:
                retry_nodes = fresh_retry_nodes
            else:
                current_healthy_node_keys = _current_healthy_node_keys()
                transient_retry_nodes = [
                    node
                    for node in retry_nodes
                    if _node_instance_key_from_node(node) in current_healthy_node_keys
                    and classify_error(
                        create_failures.get(_node_instance_key_from_node(node), ""),
                        resource_kind="task_pool",
                    )
                    == ErrorCategory.TRANSIENT_NETWORK
                    and _node_instance_key_from_node(node)
                    not in {_node_instance_key_from_node(created_node) for created_node, _pool in created}
                ]
                if not transient_retry_nodes:
                    logger.info(
                        "task pool create recovery will not retry unchanged transient nodes "
                        "because no same node_instance_id is healthy pool_name=%s attempt=%s "
                        "retry_candidates=%s current_healthy_nodes=%s",
                        effective_pool_name,
                        retry_attempt,
                        _summarize_discovered_nodes(retry_nodes),
                        sorted(current_healthy_node_keys),
                    )
                    return
                retry_nodes = transient_retry_nodes
                _emit_taskpool_notice(
                    "open retrying unchanged transient task pool nodes "
                    f"pool_name={effective_pool_name} attempt={retry_attempt} "
                    f"required={required_success_nodes} "
                    f"candidates={_summarize_discovered_nodes(retry_nodes)}"
                )
            if not retry_nodes:
                return
            all_selected_nodes.extend(retry_nodes)
            retry_desired = retry_nodes[:required_success_nodes]
            retry_fallback = retry_nodes[required_success_nodes:]
            _emit_taskpool_notice(
                "open retrying after all selected nodes failed "
                f"pool_name={effective_pool_name} attempt={retry_attempt} "
                f"required={required_success_nodes} "
                f"candidates={_summarize_discovered_nodes(retry_nodes)}"
            )
            _record_create_results(retry_desired)
            for fallback_node in retry_fallback:
                if len(created) >= required_success_nodes:
                    break
                _record_create_results([fallback_node])

        run_replica_create_recovery_loop(
            timeout_sec=max(0.1, float(timeout_sec or 0.0)),
            should_continue=_should_continue_create_recovery,
            attempt_once=_attempt_create_recovery,
            base_interval_sec=0.5,
            max_interval_sec=0.5,
        )
    if not created:
        raise RuntimeError(f"task pool create failed on all selected nodes; failures={create_failures}")
    desired_order = {
        _node_instance_key_from_node(node): index
        for index, node in enumerate(all_selected_nodes)
    }
    created.sort(key=lambda item: desired_order.get(_node_instance_key_from_node(item[0]), len(desired_order)))

    pools: Dict[str, NativeTaskPoolClient] = {}
    nodes: Dict[str, InfoCenterNode] = {}
    for node, pool in created:
        node_key = _node_instance_key_from_node(node)
        pool.node_instance_id = node_key
        pool.node_id = str(node.node_id or "")
        pools[node_key] = pool
        nodes[node_key] = node
    session = cls(
        pools=pools,
        nodes=nodes,
        task_method=entry_callable,
        job_id=job_id,
        serialization_mode=effective_policy.resolved_mode,
        policy_id=policy_id or get_default_policy_id_for_binding("taskpool_default"),
        effective_policy=effective_policy,
    )
    session.failures.update(create_failures)
    session._configure_dynamic_compensation(
        {
            "infocenter_target": infocenter_target,
            "owner_client_id": effective_owner,
            "pool_name": effective_pool_name,
            "blob": effective_blob,
            "runtime": runtime,
            "entry_module": entry_module,
            "entry_callable": entry_callable,
            "package_format": effective_package_format,
            "deps": prepared_artifact.dependency_policy,
            "managed_global_names": managed_global_names,
            "initial_globals": dict(initial_globals_values),
            "worker_count": worker_count,
            "heartbeat_timeout_sec": heartbeat_timeout_sec,
            "idle_ttl_sec": idle_ttl_sec,
            "chunk_size": chunk_size,
            "healthy_only": healthy_only,
            "tags": list(tags or ()),
            "node_ids": requested_node_ids,
            "node_instance_ids": requested_node_instance_ids,
            "node_count": compensation_target_count,
            "node_limit": node_limit,
            "timeout_sec": timeout_sec,
            "api_token": effective_api_token,
        }
    )
    session._start_keepalive()
    _emit_taskpool_notice(
        f"open success pool_name={effective_pool_name} "
        f"routes={_format_pool_route_summary(session.route_summary())}"
    )
    return session


def _build_local_task_pool(
    cls,
    *,
    job_id: str = "",
    source: Any = None,
    owner_client_id: Optional[str] = None,
    pool_name: Optional[str] = None,
    artifact: Optional[Any] = None,
    deps: Optional[Any] = None,
    runtime: str = "py3",
    entry_module: Any = "",
    entry_callable: Any = "run",
    package_format: str = "",
    resource_paths: Optional[Sequence[Any]] = None,
    managed_global_names: Optional[Sequence[str]] = None,
    initial_globals: Optional[Dict[str, object]] = None,
    worker_count: int = 1,
    heartbeat_timeout_sec: int = 0,
    idle_ttl_sec: int = 0,
    serialization_mode: str = "",
    policy_id: str = "",
) -> "TaskPool":
    from pycloud_parallel.controlplane.nodecontrol_state import NodeControlState
    initial_globals_values, effective_managed_global_names = normalize_initial_globals(initial_globals, managed_global_names)
    effective_heartbeat_timeout_sec = get_taskpool_heartbeat_timeout_sec(heartbeat_timeout_sec)

    if _taskpool_local_uses_direct_callable(
        source=source,
        artifact=artifact,
        deps=deps,
        package_format=package_format,
        resource_paths=resource_paths,
    ):
        return _build_direct_local_task_pool(
            cls,
            job_id=job_id,
            source=source,
            owner_client_id=owner_client_id,
            pool_name=pool_name,
            entry_module=entry_module,
            entry_callable=entry_callable,
            managed_global_names=effective_managed_global_names,
            initial_globals=initial_globals_values,
            worker_count=worker_count,
            heartbeat_timeout_sec=effective_heartbeat_timeout_sec,
            idle_ttl_sec=idle_ttl_sec,
            policy_id=policy_id,
        )

    module_source = source if inspect.ismodule(source) else None
    normalized_resource_paths = [item for item in list(resource_paths or ()) if str(item or "").strip()]
    if normalized_resource_paths and module_source is None:
        raise ValueError("resource_paths requires a module source")
    entry_module: Any = ""
    entry_callable: Any = "run"
    if normalized_resource_paths and module_source is not None:
        module_blob, module_filename = _prepare_code_blob(module=module_source, resource_paths=normalized_resource_paths)
        source = module_blob
        entry_module = _default_entry_module_for_module(module_source)
        package_format = _resolve_package_format(package_format, module_filename, default="py")

    normalized_artifact = _normalize_artifact_input(
        consumer_kind="task",
        source=source,
        artifact=artifact,
        deps=deps,
        runtime=runtime,
        entry_module=entry_module,
        entry_callable=entry_callable,
        package_format=package_format,
        managed_global_names=effective_managed_global_names,
    )
    prepared_artifact = _prepare_artifact(normalized_artifact, consumer_kind="task")
    effective_owner = str(owner_client_id or f"local-client-{_get_local_ip()}").strip()
    effective_pool_name = str(pool_name or f"local-task-pool-{uuid.uuid4().hex[:10]}").strip()
    effective_worker_count = max(1, int(worker_count or 1))
    effective_policy_id = str(policy_id or get_default_policy_id_for_binding("taskpool_default")).strip()
    effective_policy = resolve_effective_policy(
        get_policy_profile(effective_policy_id),
        requested_mode=LOCAL_IPC_SERIALIZATION_MODE,
        context="taskpool_session",
    )
    node = NodeControlState(
        node_id=f"{effective_pool_name}-local",
        worker_capacity=effective_worker_count,
        service_worker_capacity=1,
        task_pool_worker_capacity=effective_worker_count,
        service_http_bind="",
        enable_internal_executor=True,
        enable_service_session=False,
    )
    try:
        pool = node.create_task_pool(
            owner_client_id=effective_owner,
            pool_name=effective_pool_name,
            sha256=prepared_artifact.content_sha256,
            runtime=prepared_artifact.runtime,
            entry_module=prepared_artifact.entry_module,
            entry_callable=prepared_artifact.entry_callable,
            package_format=prepared_artifact.package_format,
            dependency_policy_mode=prepared_artifact.dependency_policy_mode,
            dependency_allowlist=list(prepared_artifact.dependency_allowlist),
            managed_global_names=list(prepared_artifact.managed_global_names),
            initial_globals=initial_globals_values,
            worker_count=effective_worker_count,
            heartbeat_timeout_sec=effective_heartbeat_timeout_sec,
            idle_ttl_sec=idle_ttl_sec,
            chunks=[prepared_artifact.blob],
        )
        adapter = _LocalTaskPoolNodeClient(node, pool=pool)
        session = _build_local_taskpool_session(
            cls,
            adapter=adapter,
            task_method=prepared_artifact.entry_callable,
            job_id=job_id,
            policy_id=effective_policy_id,
            effective_policy=effective_policy,
        )
    except Exception:
        node.close()
        raise
    session._start_keepalive()
    _emit_taskpool_notice(
        f"local open success pool_name={effective_pool_name} "
        f"routes={_format_pool_route_summary(session.route_summary())}"
    )
    return session


def _build_local_taskpool_session(
    cls,
    *,
    adapter: "_LocalTaskPoolNodeClient",
    task_method: str,
    job_id: str,
    policy_id: str,
    effective_policy: EffectivePolicy,
) -> "TaskPool":
    native = NativeTaskPoolClient(
        _client=adapter,
        owner_client_id=adapter.owner_client_id,
        pool_id=adapter.pool_id,
        pool_token=adapter.pool_token,
        code_version=adapter.code_version,
        worker_count=adapter.worker_count,
        heartbeat_timeout_sec=adapter.heartbeat_timeout_sec,
        pool_name=adapter.pool_name,
        idle_ttl_sec=adapter.idle_ttl_sec,
        node_instance_id=adapter.node_instance_id,
        node_id=adapter.node_id,
        status=adapter.status,
        created_at=adapter.created_at,
        last_heartbeat_at=adapter.last_heartbeat_at,
        lease_expire_at=adapter.lease_expire_at,
    )
    node_info = InfoCenterNode(
        node_instance_id=adapter.node_instance_id,
        node_id=adapter.node_id,
        control_addr="local",
        healthy=True,
        capacity=adapter.worker_count,
        queue_capacity=0,
        queued=0,
        inflight=0,
        credit=adapter.worker_count,
        task_pool_worker_capacity=adapter.worker_count,
        task_pool_worker_available=adapter.worker_count,
    )
    return cls(
        pools={adapter.node_instance_id: native},
        nodes={adapter.node_instance_id: node_info},
        task_method=task_method,
        job_id=job_id,
        serialization_mode=LOCAL_IPC_SERIALIZATION_MODE,
        policy_id=policy_id,
        effective_policy=effective_policy,
    )


def _resolve_direct_local_task_callable(source: Any, *, entry_module: Any = "", entry_callable: Any = "run") -> Tuple[Callable[..., Any], str, str]:
    method_name = _local_direct_callable_name(source, entry_callable)
    if callable(source) and not inspect.ismodule(source):
        module_name = _local_direct_module_name(source, entry_module)
        return source, module_name, method_name
    module_name = _local_direct_module_name(source, entry_module)
    if not module_name:
        raise ValueError("local TaskPool direct mode requires source=module/callable or entry_module")
    module = importlib.import_module(module_name)
    fn = getattr(module, method_name, None)
    if not callable(fn):
        raise ValueError(f"task callable not found in module {module_name!r}: {method_name!r}")
    return fn, module_name, method_name


def _build_direct_local_task_pool(
    cls,
    *,
    job_id: str = "",
    source: Any = None,
    owner_client_id: Optional[str] = None,
    pool_name: Optional[str] = None,
    entry_module: Any = "",
    entry_callable: Any = "run",
    managed_global_names: Optional[Sequence[str]] = None,
    initial_globals: Optional[Dict[str, object]] = None,
    worker_count: int = 1,
    heartbeat_timeout_sec: int = 0,
    idle_ttl_sec: int = 0,
    policy_id: str = "",
) -> "TaskPool":
    fn, module_name, method_name = _resolve_direct_local_task_callable(
        source,
        entry_module=entry_module,
        entry_callable=entry_callable,
    )
    effective_owner = str(owner_client_id or f"local-client-{_get_local_ip()}").strip()
    effective_pool_name = str(pool_name or module_name.rsplit(".", 1)[-1] or f"local-task-pool-{uuid.uuid4().hex[:10]}").strip()
    effective_worker_count = max(1, int(worker_count or 1))
    effective_heartbeat_timeout_sec = get_taskpool_heartbeat_timeout_sec(heartbeat_timeout_sec)
    effective_policy_id = str(policy_id or get_default_policy_id_for_binding("taskpool_default")).strip()
    effective_policy = resolve_effective_policy(
        get_policy_profile(effective_policy_id),
        requested_mode=LOCAL_IPC_SERIALIZATION_MODE,
        context="taskpool_session",
    )
    node_id = f"{effective_pool_name}-local"
    node_instance_id = f"{node_id}-{uuid.uuid4().hex[:10]}"
    adapter = _DirectLocalTaskPoolNodeClient(
        node_id=node_id,
        node_instance_id=node_instance_id,
        pool_name=effective_pool_name,
        owner_client_id=effective_owner,
        worker_count=effective_worker_count,
        heartbeat_timeout_sec=effective_heartbeat_timeout_sec,
        idle_ttl_sec=idle_ttl_sec,
        fn=fn,
        managed_global_names=managed_global_names or (),
        initial_globals=dict(initial_globals or {}),
    )
    session = _build_local_taskpool_session(
        cls,
        adapter=adapter,
        task_method=method_name,
        job_id=job_id,
        policy_id=effective_policy_id,
        effective_policy=effective_policy,
    )
    session._start_keepalive()
    _emit_taskpool_notice(
        f"local open direct success pool_name={effective_pool_name} "
        f"entry={module_name}.{method_name} routes={_format_pool_route_summary(session.route_summary())}"
    )
    return session


__all__ = [
    "TaskPool",
]


class TaskPool(_TaskPoolSessionBase):
    """V1 task-pool session."""

    @classmethod
    def open(
        cls,
        *,
        target: str,
        job_id: str = "",
        source: Any = None,
        owner_client_id: Optional[str] = None,
        pool_name: Optional[str] = None,
        artifact: Optional[Any] = None,
        deps: Optional[Any] = None,
        runtime: str = "py3",
        package_format: str = "",
        resource_paths: Optional[Sequence[Any]] = None,
        managed_global_names: Optional[Sequence[str]] = None,
        initial_globals: Optional[Dict[str, object]] = None,
        worker_count: int = 1,
        heartbeat_timeout_sec: int = 0,
        idle_ttl_sec: int = 0,
        chunk_size: int = OBJECT_CHUNK_SIZE_BYTES,
        healthy_only: bool = True,
        tags: Optional[Sequence[str]] = None,
        node_ids: Optional[Sequence[str]] = None,
        node_instance_ids: Optional[Sequence[str]] = None,
        node_count: int = 0,
        node_limit: int = 100,
        timeout_sec: float = 10.0,
        serialization_mode: str = "",
        policy_id: str = "",
        api_token: str = "",
    ) -> "TaskPool":
        """Product-facing open action for V1 task pools.

        Default path: ``TaskPool.open(target=\"127.0.0.1:50051\", source=my_module, ...)``.
        Advanced path: ``TaskPool.open(artifact=Artifact(...), ...)``.
        """
        if str(target or "").strip().lower() == "local":
            return _build_local_task_pool(
                cls,
                job_id=job_id,
                source=source,
                owner_client_id=owner_client_id,
                pool_name=pool_name,
                artifact=artifact,
                deps=deps,
                runtime=runtime,
                package_format=package_format,
                resource_paths=resource_paths,
                managed_global_names=managed_global_names,
                initial_globals=initial_globals,
                worker_count=worker_count,
                heartbeat_timeout_sec=heartbeat_timeout_sec,
                idle_ttl_sec=idle_ttl_sec,
                serialization_mode=serialization_mode,
                policy_id=policy_id,
            )
        effective_target = _resolve_public_target_arg(
            target=target,
            action_name="TaskPool.open()",
        )
        return cls._from_infocenter(
            infocenter_target=effective_target,
            job_id=job_id,
            source=source,
            owner_client_id=owner_client_id,
            pool_name=pool_name,
            artifact=artifact,
            deps=deps,
            runtime=runtime,
            package_format=package_format,
            resource_paths=resource_paths,
            managed_global_names=managed_global_names,
            initial_globals=initial_globals,
            worker_count=worker_count,
            heartbeat_timeout_sec=heartbeat_timeout_sec,
            idle_ttl_sec=idle_ttl_sec,
            chunk_size=chunk_size,
            healthy_only=healthy_only,
            tags=tags,
            node_ids=node_ids,
            node_instance_ids=node_instance_ids,
            node_count=node_count,
            node_limit=node_limit,
            timeout_sec=timeout_sec,
            serialization_mode=serialization_mode,
            policy_id=policy_id,
            api_token=api_token,
        )

    @classmethod
    def _from_infocenter(
        cls,
        *,
        infocenter_target: str,
        job_id: str = "",
        source: Any = None,
        owner_client_id: Optional[str] = None,
        pool_name: Optional[str] = None,
        artifact: Optional[Any] = None,
        deps: Optional[Any] = None,
        runtime: str = "py3",
        entry_module: Any = "",
        entry_callable: Any = "run",
        package_format: str = "",
        resource_paths: Optional[Sequence[Any]] = None,
        managed_global_names: Optional[Sequence[str]] = None,
        initial_globals: Optional[Dict[str, object]] = None,
        worker_count: int = 1,
        heartbeat_timeout_sec: int = 0,
        idle_ttl_sec: int = 0,
        chunk_size: int = OBJECT_CHUNK_SIZE_BYTES,
        healthy_only: bool = True,
        tags: Optional[Sequence[str]] = None,
        node_ids: Optional[Sequence[str]] = None,
        node_instance_ids: Optional[Sequence[str]] = None,
        node_count: int = 0,
        node_limit: int = 100,
        timeout_sec: float = 10.0,
        serialization_mode: str = "",
        policy_id: str = "",
        api_token: str = "",
    ) -> "TaskPool":
        """Low-level entry; prefer ``TaskPool.open(...)``.

        `policy_id` here is a deployment/control-plane input. Product callers
        should normally express only `serialization_mode`; the session will
        expose the frozen `effective_policy` that actually took effect.
        """
        return _build_task_pool_from_infocenter(
            cls,
            infocenter_target=infocenter_target,
            job_id=job_id,
            source=source,
            owner_client_id=owner_client_id,
            pool_name=pool_name,
            artifact=artifact,
            deps=deps,
            runtime=runtime,
            entry_module=entry_module,
            entry_callable=entry_callable,
            package_format=package_format,
            resource_paths=resource_paths,
            managed_global_names=managed_global_names,
            initial_globals=initial_globals,
            worker_count=worker_count,
            heartbeat_timeout_sec=heartbeat_timeout_sec,
            idle_ttl_sec=idle_ttl_sec,
            chunk_size=chunk_size,
            healthy_only=healthy_only,
            tags=tags,
            node_ids=node_ids,
            node_instance_ids=node_instance_ids,
            node_count=node_count,
            node_limit=node_limit,
            timeout_sec=timeout_sec,
            serialization_mode=serialization_mode,
            policy_id=policy_id,
            api_token=api_token,
        )
