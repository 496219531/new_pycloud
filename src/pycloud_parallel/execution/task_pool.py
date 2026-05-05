from __future__ import annotations

"""Authoritative V1 task-pool implementation."""

import asyncio
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
import contextlib
from dataclasses import dataclass, replace
import hashlib
import inspect
import logging
import math
from pathlib import Path
import sys
import tempfile
import threading
import time
from typing import Any, AsyncIterator, Callable, Dict, Iterable, Iterator, List, Optional, Sequence, Set, Tuple, Union
import uuid

from pycloud_parallel.controlplane.artifact import (
    _default_entry_module_for_module,
    _normalize_artifact_input,
    _prepare_artifact,
    _resolve_package_format,
)
from pycloud_parallel.controlplane.config import OBJECT_CHUNK_SIZE_BYTES
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
from pycloud_parallel.controlplane.serialization_mode import resolve_effective_serialization_mode
from pycloud_parallel.controlplane.session_model import ExecutionSessionStatus
from pycloud_parallel.controlplane.replica_client import NativeTaskPoolClient
from pycloud_parallel.controlplane.session_handle import ExecutionReplicaHandle
from pycloud_parallel.controlplane.serialization import encode_transport_payload_bytes, serialize_inline_payload
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
from pycloud_parallel.execution.scheduler import (
    SchedulerCandidate,
    SchedulerState,
    resolve_taskpool_strategy,
    select_one_candidate,
)
from pycloud_parallel.execution.support import (
    _get_local_ip,
    _prepare_code_blob,
    _prepare_task_payload_for_submit,
    _put_data_via_clients,
    _resolve_public_target_arg,
)
from pycloud_parallel.data.ref import DataRef, maybe_data_ref, normalize_object_format, object_id_from_sha256_hex
from pycloud_parallel.proto.v1 import pycloud_v1_pb2 as pb2


logger = logging.getLogger(__name__)
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

    def next(self) -> Optional[Tuple[int, Dict[str, object]]]:
        if self._retry_payloads:
            index, payload = self._retry_payloads.popleft()
            return int(index), dict(payload or {})
        if self._input_exhausted:
            return None
        try:
            raw_payload = next(self._payload_iter)
        except StopIteration:
            self._input_exhausted = True
            return None
        if not isinstance(raw_payload, dict):
            raise TypeError("payloads must yield dict items")
        payload = dict(raw_payload)
        index = self._next_index
        self._next_index += 1
        return index, payload

    def requeue_front(self, items: Sequence[Tuple[int, Dict[str, object], Any]]) -> None:
        for index, payload, _item in reversed(list(items)):
            self._retry_payloads.appendleft((int(index), dict(payload or {})))


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


def _infocenter_client(*args, **kwargs):
    from pycloud_parallel.controlplane.infocenter_client import InfoCenterClient

    return InfoCenterClient(*args, **kwargs)


class _LocalTaskPoolNodeClient:
    def __init__(self, state) -> None:
        self._state = state
        self.target = ""
        self.node_id = str(getattr(state, "node_id", "") or "")
        self.node_instance_id = str(getattr(state, "node_instance_id", "") or "")

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
        del chunk_size, kwargs
        path = Path(file_path)
        data = path.read_bytes()
        fmt = normalize_object_format(format, source_name=path.name, default="bin")
        return self.upload_object_from_bytes(blob=data, format=fmt)

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
    ) -> pb2.HeartbeatTaskPoolResponse:
        del seq
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

    def _after_keepalive_tick(self) -> None:
        spec = self._compensation_spec
        if not spec:
            return
        now = time.monotonic()
        interval_sec = max(5.0, float(spec.get("check_interval_sec", 15.0) or 15.0))
        if now - float(self._last_compensation_attempt_at or 0.0) < interval_sec:
            return
        self._last_compensation_attempt_at = now
        self.try_compensate_replicas()

    def try_compensate_replicas(self) -> int:
        spec = self._compensation_spec
        if not spec or self._closed:
            return 0
        if not self._compensation_lock.acquire(blocking=False):
            return 0
        try:
            desired = max(0, int(spec.get("node_count", 0) or 0))
            active = {str(node_id) for node_id in self._active_replica_ids if str(node_id)}
            failed = {str(node_id) for node_id in self.failures.keys() if str(node_id)}
            if desired <= 0 or len(active) >= desired:
                return 0
            excluded = active | failed
            with _infocenter_client(spec["infocenter_target"], timeout_sec=float(spec.get("timeout_sec", 10.0) or 10.0)) as infocenter:
                selected_nodes = list(
                    infocenter.select_task_nodes(
                        healthy_only=bool(spec.get("healthy_only", True)),
                        tags=list(spec.get("tags") or ()),
                        node_ids=list(spec.get("node_ids") or ()),
                        node_instance_ids=list(spec.get("node_instance_ids") or ()),
                        node_count=desired,
                        limit=max(desired, int(spec.get("node_limit", 100) or 100)),
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
            if not candidates:
                return 0
            missing = max(0, desired - len(active))

            def _create_pool_on_node(node: InfoCenterNode) -> Tuple[str, InfoCenterNode, NativeTaskPoolClient]:
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
                    worker_count=max(1, int(spec.get("worker_count", 1) or 1)),
                    heartbeat_timeout_sec=max(5, int(spec.get("heartbeat_timeout_sec", 30) or 30)),
                    idle_ttl_sec=max(0, int(spec.get("idle_ttl_sec", 0) or 0)),
                    chunk_size=max(1, int(spec.get("chunk_size", OBJECT_CHUNK_SIZE_BYTES) or OBJECT_CHUNK_SIZE_BYTES)),
                )
                node_key = _node_instance_key_from_node(node)
                pool.node_instance_id = node_key
                pool.node_id = str(node.node_id or "")
                return node_key, node, pool

            created: List[Tuple[str, InfoCenterNode, NativeTaskPoolClient]] = []
            for node in candidates[:missing]:
                try:
                    created.append(_create_pool_on_node(node))
                except Exception as exc:
                    self.failures[_node_instance_key_from_node(node)] = repr(exc)
            if not created:
                return 0
            added = 0
            with self._pool_lock:
                for node_key, node, pool in created:
                    if node_key in self._pools:
                        _close_task_pool_replica(pool, reason="duplicate compensated task pool")
                        with contextlib.suppress(Exception):
                            pool._client.close()  # noqa: SLF001
                        continue
                    self._pools[node_key] = pool
                    self.nodes[node_key] = node
                    self.failures.pop(node_key, None)
                    self._active_replica_ids.add(node_key)
                    self._active_nodes.add(node_key)
                    self._submit_breaker_states.setdefault(node_key, CandidateBreakerState())
                    added += 1
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
            active = {str(node_id) for node_id in list(getattr(self, "_active_nodes") or [])}
            return [
                node_id
                for node_id in ordered_node_ids
                if node_id in active and self._pool_candidate_allowed(node_id)
            ]
        if hasattr(self, "_active_replica_ids"):
            active = {str(node_id) for node_id in list(getattr(self, "_active_replica_ids") or [])}
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
        if should_use_raw_bytes_payload(
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
        resp = self._pools[node_id].submit_tasks(items, job_id=str(job_id or self.job_id).strip())
        self._register_pending_task_ids(resp.accepted, node_id=node_id)
        return resp

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
                self._build_task_submit_item(
                    node_id=target_node_id,
                    payload=dict(payload or {}),
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
            yield {**dict(payload), **shared}

    def _item_with_index(self, item: ExecutionItem, *, index: int, key: Union[int, str]) -> ExecutionItem:
        return replace(item, index=int(index), key=key)

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
                try:
                    resp = pool.pull_results(limit=per_pull_limit, wait_ms=0, cursor="")
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
        wait_ms: int = 500,
        limit: int = 100,
        job_id: str = "",
    ) -> Iterator[pb2.TaskResult]:
        self._assert_session_available("iter_results")
        for _node_id, item in self._iter_raw_results(
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
        for item in self._iter_execution_items(
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
        wait_ms: int = 500,
        limit: int = 100,
        job_id: str = "",
        task_ids: Optional[Set[str]] = None,
        **shared_kwargs,
    ) -> Iterator[ExecutionItem]:
        """Iterate task results as structured items.

        When ``payloads is None``, this only consumes already-submitted results from the current
        session. When ``payloads`` is provided, this submits that batch and yields `ExecutionItem`
        objects for the batch.
        """
        self._assert_session_available("iter_items")
        if payloads is None:
            yield from self._iter_execution_items(
                max_count=max_count,
                timeout_sec=timeout_sec,
                wait_ms=wait_ms,
                limit=limit,
                job_id=job_id,
                task_ids=task_ids,
            )
            return
        normalized_payloads = self._merge_payloads_with_shared_kwargs(payloads, shared_kwargs=dict(shared_kwargs))
        resolved_max_in_flight = self._resolve_max_in_flight(max_in_flight)
        for item in self.imap_unordered(
            normalized_payloads,
            task_method=task_method,
            strategy=strategy,
            max_in_flight=resolved_max_in_flight,
            receive_batch=max(1, min(resolved_max_in_flight, 32)),
            submit_timeout_sec=max(0.1, float(timeout_sec)),
            result_timeout_sec=max(0.1, float(timeout_sec)),
            wait_ms=wait_ms,
            raise_on_error=False,
            node_window_factor=2.0,
            return_items=True,
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
        wait_ms: int = 500,
        limit: int = 100,
        job_id: str = "",
        task_ids: Optional[Set[str]] = None,
        **shared_kwargs,
    ) -> List[ExecutionItem]:
        items = list(
            self.iter_items(
                payloads,
                task_method=task_method,
                strategy=strategy,
                max_in_flight=max_in_flight,
                max_count=max_count,
                timeout_sec=timeout_sec,
                wait_ms=wait_ms,
                limit=limit,
                job_id=job_id,
                task_ids=task_ids,
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
        wait_ms: int = 500,
        limit: int = 100,
        job_id: str = "",
        task_ids: Optional[Set[str]] = None,
        **shared_kwargs,
    ) -> AsyncIterator[ExecutionItem]:
        """Async counterpart of :meth:`iter_items` with the same dual-mode semantics."""
        iterator = self.iter_items(
            payloads,
            task_method=task_method,
            strategy=strategy,
            max_in_flight=max_in_flight,
            timeout_sec=timeout_sec,
            max_count=max_count,
            wait_ms=wait_ms,
            limit=limit,
            job_id=job_id,
            task_ids=task_ids,
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
        wait_ms: int = 500,
        limit: int = 100,
        job_id: str = "",
        task_ids: Optional[Set[str]] = None,
        **shared_kwargs,
    ) -> List[ExecutionItem]:
        """Async counterpart of :meth:`collect_items` with the same dual-mode semantics."""
        return await asyncio.to_thread(
            lambda: self.collect_items(
                payloads,
                task_method=task_method,
                strategy=strategy,
                max_in_flight=max_in_flight,
                timeout_sec=timeout_sec,
                max_count=max_count,
                wait_ms=wait_ms,
                limit=limit,
                job_id=job_id,
                task_ids=task_ids,
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
        wait_ms: int = 500,
        limit: int = 100,
        job_id: str = "",
    ) -> Sequence[pb2.TaskResult]:
        max_count = max(0, int(expected_count or 0))
        return list(
            self.iter_results(
                max_count=(max_count if max_count > 0 else None),
                timeout_sec=timeout_sec,
                wait_ms=wait_ms,
                limit=limit,
                job_id=job_id,
            )
        )

    def wait_for_data(
        self,
        *,
        expected_count: int = 0,
        timeout_sec: float = 30.0,
    ) -> Sequence[Any]:
        max_count = max(0, int(expected_count or 0))
        return [
            data
            for _task_id, data in self.iter_data(
                max_count=(max_count if max_count > 0 else None),
                timeout_sec=timeout_sec,
                raise_on_error=True,
            )
        ]

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
                item = self._build_task_submit_item(
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
                scheduler_failures[node_id] = repr(exc)
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
    ) -> Tuple[List[ExecutionItem], Dict[str, int]]:
        completed_items: List[ExecutionItem] = []
        freed_by_node: Dict[str, int] = {}

        for node_id in ordered_node_ids:
            if node_id in disabled_submit_nodes and int(inflight_by_node.get(node_id, 0) or 0) <= 0:
                continue
            pull_limit = max(1, int(inflight_by_node.get(node_id, 0) or 1))
            try:
                resp = self._pools[node_id].pull_results(limit=pull_limit, wait_ms=0, cursor="")
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
            item = self._build_task_submit_item(
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
        wait_ms: int = 500,
        raise_on_error: bool = True,
        max_infra_retries: int = 1,
        retry_backoff_ms: int = 0,
        node_window_factor: float = 2.0,
    ) -> Iterator[Union[Tuple[int, Any], ExecutionItem]]:
        if self._closed:
            raise RuntimeError("task pool session is closed")
        self._enter_exclusive_mode("imap_unordered", require_clean=True)
        try:
            self._ensure_method(str(task_method or self._task_method).strip() or self._task_method)
            max_pending = self._resolve_max_in_flight(max_in_flight)
            max_receive = max(1, int(receive_batch or 1))
            submit_timeout_sec = float(submit_timeout_sec if submit_timeout_sec is not None else timeout_sec)
            result_timeout_sec = float(result_timeout_sec if result_timeout_sec is not None else timeout_sec)
            wait_ms = int(wait_ms or 500)
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
            if self._pending_result_count() <= 0 and not payload_buffer.has_retry and payload_buffer.exhausted:
                return

            while True:
                yielded = 0
                while ready_items and yielded < max_receive:
                    item = ready_items.popleft()
                    if not item.ok:
                        if raise_on_error:
                            if not cancelled_for_error:
                                with contextlib.suppress(Exception):
                                    self.cancel_job(reason="imap_unordered task failure", job_id=self.job_id)
                                cancelled_for_error = True
                            raise RuntimeError(item.error_message or f"task failed: {item.task_id}")
                        yield item if return_items else (item.index, None)
                    else:
                        yield item if return_items else (item.index, item.data)
                    yielded += 1
                if yielded > 0:
                    continue

                if payload_buffer.exhausted and not payload_buffer.has_retry and self._pending_result_count() <= 0:
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
                        wait_deadline = time.time() + max(0.1, float(result_timeout_sec))
                        continue
                    if not payload_buffer.has_retry and not payload_buffer.exhausted:
                        next_payload = payload_buffer.next()
                        if next_payload is None:
                            continue
                        payload_buffer.requeue_front([(next_payload[0], next_payload[1], None)])
                    if payload_buffer.has_retry or not payload_buffer.exhausted:
                        failure_suffix = f"; failures={scheduler_failures}" if scheduler_failures else ""
                        raise RuntimeError(f"imap_unordered could not submit tasks to any active task pool node{failure_suffix}")
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
                    continue

                if time.time() >= wait_deadline:
                    raise TimeoutError(
                        f"imap_unordered did not receive results before timeout; pending_task_ids={self._pending_result_count()}"
                    )
                time.sleep(max(0.01, min(0.1, wait_ms / 1000.0 if wait_ms > 0 else 0.02)))
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
    ) -> Iterator[Union[Tuple[int, Any], ExecutionItem]]:
        """Yield ``(index, result_or_none)`` in completion order for a submitted batch."""
        for item in self.iter_items(
            payloads,
            task_method=task_method,
            strategy=strategy,
            max_in_flight=max_in_flight,
            timeout_sec=timeout_sec,
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
    ) -> AsyncIterator[Union[Tuple[int, Any], ExecutionItem]]:
        """Async counterpart of :meth:`unordered` with the same return shape."""
        async for item in self.aiter_items(
            payloads,
            task_method=task_method,
            strategy=strategy,
            max_in_flight=max_in_flight,
            timeout_sec=timeout_sec,
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
    ) -> int:
        if not callable(handle):
            raise TypeError("handle must be callable")
        processed = 0
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
        **shared_kwargs,
    ) -> Sequence[Any]:
        normalized_arg = str(arg_name or "value").strip() or "value"
        shared = dict(shared_kwargs)
        payloads = ({normalized_arg: value, **shared} for value in values)
        items = self.collect_items(
            payloads,
            task_method=task_method,
            strategy=strategy,
            max_in_flight=self._resolve_max_in_flight(max_in_flight),
            timeout_sec=timeout_sec,
        )
        return [item.result if item.ok else None for item in items]

    async def amap(
        self,
        values: Iterable[Any],
        *,
        arg_name: str = "value",
        task_method: str = "",
        strategy: str = "taskpool_default",
        max_in_flight: Optional[int] = None,
        timeout_sec: float = 30.0,
        **shared_kwargs,
    ) -> Sequence[Any]:
        return await asyncio.to_thread(
            lambda: self.map(
                values,
                arg_name=arg_name,
                task_method=task_method,
                strategy=strategy,
                max_in_flight=max_in_flight,
                timeout_sec=timeout_sec,
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

    def status(self) -> ExecutionSessionStatus:
        return super().status()

    def is_alive(self) -> bool:
        return (not self._closed) and (not self.failed) and any(
            node_id in self._active_nodes for node_id in self._pools
        )

    def put_data(
        self,
        data: Any,
        *,
        format: str = "",
        chunk_size: int = OBJECT_CHUNK_SIZE_BYTES,
        serialization_mode: str = "",
    ) -> DataRef:
        if self._closed:
            raise RuntimeError("task pool session is closed")
        pools_snapshot = list(self._pools.values())
        active_clients = [pool._client for pool in pools_snapshot]  # noqa: SLF001
        effective_serialization_mode = resolve_effective_serialization_mode(
            request_mode=serialization_mode,
            context="object_upload",
            frozen_mode=self._serialization_mode,
        )
        return _put_data_via_clients(
            active_clients,
            data,
            format=format,
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

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._stop_keepalive()
        for pool in self._pools.values():
            _close_task_pool_replica(pool, reason="task pool session close")
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
        normalized = self._ensure_method(method)
        return getattr(self, normalized).sync(**kwargs)

    async def call(self, method: str, **kwargs) -> Any:
        normalized = self._ensure_method(method)
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, lambda: getattr(self, normalized).sync(**kwargs))

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
    serialization_mode: str = "",
    policy_id: str = "",
) -> "TaskPool":
    module_source = source if inspect.ismodule(source) else None
    normalized_resource_paths = [item for item in list(resource_paths or ()) if str(item or "").strip()]
    if normalized_resource_paths and module_source is None:
        raise ValueError("resource_paths requires a module source")
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
        managed_global_names=managed_global_names,
    )
    prepared_artifact = _prepare_artifact(
        normalized_artifact,
        consumer_kind="task",
    )
    effective_blob = prepared_artifact.blob
    runtime = prepared_artifact.runtime
    entry_module = prepared_artifact.entry_module
    entry_callable = prepared_artifact.entry_callable
    effective_package_format = prepared_artifact.package_format
    dependency_allowlist = list(prepared_artifact.dependency_allowlist)
    managed_global_names = list(prepared_artifact.managed_global_names)

    effective_owner = str(owner_client_id or f"client-{_get_local_ip()}").strip()
    requested_count = max(0, int(node_count or 0))
    requested_node_ids = [str(node_id).strip() for node_id in list(node_ids or ()) if str(node_id).strip()]
    requested_node_instance_ids = [str(node_id).strip() for node_id in list(node_instance_ids or ()) if str(node_id).strip()]
    compensation_target_count = requested_count or len(requested_node_instance_ids) or len(requested_node_ids)
    fetch_limit = requested_count if requested_count > 0 else node_limit
    with _infocenter_client(infocenter_target, timeout_sec=timeout_sec) as infocenter:
        selected_nodes = list(
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
    if not selected_nodes:
        raise RuntimeError("no task pool nodes selected from InfoCenter")
    desired_nodes = selected_nodes[:requested_count] if requested_count > 0 else selected_nodes
    effective_pool_name = str(pool_name or f"task-pool-{uuid.uuid4().hex[:10]}").strip()
    effective_policy = resolve_effective_policy(
        get_policy_profile(policy_id or get_default_policy_id_for_binding("taskpool_default")),
        requested_mode=serialization_mode,
        context="taskpool_session",
    )

    def _create_pool_on_node(node: InfoCenterNode) -> Tuple[InfoCenterNode, NativeTaskPoolClient]:
        target = _node_control_target_for_node(node)
        client = _new_node_control_client(target, timeout_sec=timeout_sec)
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
                worker_count=worker_count,
                heartbeat_timeout_sec=heartbeat_timeout_sec,
                idle_ttl_sec=idle_ttl_sec,
                chunk_size=chunk_size,
            )
        except Exception:
            with contextlib.suppress(Exception):
                client.close()
            raise
        return node, pool

    created: List[Tuple[InfoCenterNode, NativeTaskPoolClient]] = []
    create_failures: Dict[str, str] = {}
    if len(desired_nodes) == 1:
        try:
            created.append(_create_pool_on_node(desired_nodes[0]))
        except Exception as exc:
            create_failures[_node_instance_key_from_node(desired_nodes[0])] = repr(exc)
    else:
        with ThreadPoolExecutor(max_workers=max(1, len(desired_nodes)), thread_name_prefix="taskpool-create") as executor:
            futures = {executor.submit(_create_pool_on_node, node): node for node in desired_nodes}
            for future in as_completed(futures):
                node = futures[future]
                try:
                    created.append(future.result())
                except Exception as exc:
                    node_key = _node_instance_key_from_node(node)
                    create_failures[node_key] = repr(exc)
                    logger.warning(
                        "task pool replica create failed pool_name=%s node_id=%s node_instance_id=%s err=%r",
                        effective_pool_name,
                        getattr(node, "node_id", ""),
                        node_key,
                        exc,
                    )
    if not created:
        raise RuntimeError(f"task pool create failed on all selected nodes; failures={create_failures}")
    desired_order = {
        _node_instance_key_from_node(node): index
        for index, node in enumerate(desired_nodes)
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
    worker_count: int = 1,
    heartbeat_timeout_sec: int = 30,
    idle_ttl_sec: int = 0,
    serialization_mode: str = "",
    policy_id: str = "",
) -> "TaskPool":
    from pycloud_parallel.controlplane.nodecontrol_state import NodeControlState

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
        managed_global_names=managed_global_names,
    )
    prepared_artifact = _prepare_artifact(normalized_artifact, consumer_kind="task")
    effective_owner = str(owner_client_id or f"local-client-{_get_local_ip()}").strip()
    effective_pool_name = str(pool_name or f"local-task-pool-{uuid.uuid4().hex[:10]}").strip()
    effective_worker_count = max(1, int(worker_count or 1))
    effective_policy_id = str(policy_id or get_default_policy_id_for_binding("taskpool_default")).strip()
    effective_policy = resolve_effective_policy(
        get_policy_profile(effective_policy_id),
        requested_mode=serialization_mode,
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
            worker_count=effective_worker_count,
            heartbeat_timeout_sec=heartbeat_timeout_sec,
            idle_ttl_sec=idle_ttl_sec,
            chunks=[prepared_artifact.blob],
        )
        adapter = _LocalTaskPoolNodeClient(node)
        native = NativeTaskPoolClient(
            _client=adapter,
            owner_client_id=pool.owner_client_id,
            pool_id=pool.pool_id,
            pool_token=pool.pool_token,
            code_version=pool.code_version,
            worker_count=pool.worker_count,
            heartbeat_timeout_sec=pool.heartbeat_timeout_sec,
            pool_name=pool.pool_name,
            idle_ttl_sec=pool.idle_ttl_sec,
            node_instance_id=adapter.node_instance_id,
            node_id=adapter.node_id,
            status=pool.status,
            created_at=pool.created_at,
            last_heartbeat_at=pool.last_heartbeat_at,
            lease_expire_at=pool.lease_expire_at,
        )
        node_info = InfoCenterNode(
            node_instance_id=adapter.node_instance_id,
            node_id=adapter.node_id,
            control_addr="local",
            healthy=True,
            capacity=effective_worker_count,
            queue_capacity=0,
            queued=0,
            inflight=0,
            credit=effective_worker_count,
            task_pool_worker_capacity=effective_worker_count,
            task_pool_worker_available=effective_worker_count,
        )
        session = cls(
            pools={adapter.node_instance_id: native},
            nodes={adapter.node_instance_id: node_info},
            task_method=prepared_artifact.entry_callable,
            job_id=job_id,
            serialization_mode=effective_policy.resolved_mode,
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
        serialization_mode: str = "",
        policy_id: str = "",
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
        serialization_mode: str = "",
        policy_id: str = "",
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
        )
