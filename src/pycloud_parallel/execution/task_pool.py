from __future__ import annotations

"""Authoritative V1 task-pool implementation."""

import asyncio
from collections import deque
import contextlib
from dataclasses import replace
import inspect
import logging
import math
import os
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
    should_use_transport_payload_bytes,
)
from pycloud_parallel.controlplane.infocenter_client import InfoCenterNode, _node_instance_key_from_node
from pycloud_parallel.controlplane.policy_profile import (
    get_default_policy_id_for_binding,
    get_policy_profile,
)
from pycloud_parallel.controlplane.serialization_mode import resolve_effective_serialization_mode
from pycloud_parallel.controlplane.session_model import ExecutionSessionStatus
from pycloud_parallel.controlplane.replica_client import NativeTaskPoolClient
from pycloud_parallel.controlplane.session_handle import ExecutionReplicaHandle
from pycloud_parallel.controlplane.serialization import detect_transport_mode, serialize_inline_payload, struct_to_python
from pycloud_parallel.controlplane.serialization import (
    decode_transport_payload_bytes,
    encode_transport_payload_bytes,
)
from pycloud_parallel.controlplane.payload_transport import decode_result_from_transport
from pycloud_parallel.controlplane.task_backend import NativeTaskBackend, _TaskPoolCallProxy
from pycloud_parallel.execution.base import ExecutionItem, TaskExecutionSession
from pycloud_parallel.execution.failover import (
    CandidateBreakerState,
    REMOTE_INFRA_FAILED,
    SUBMIT_FAILED,
    before_probe,
    candidate_allowed,
    mark_candidate_failure,
    mark_candidate_success,
)
from pycloud_parallel.execution.scheduler import (
    TASKPOOL_DEFAULT,
    SchedulerCandidate,
    SchedulerState,
    resolve_taskpool_strategy,
    select_one_candidate,
)
from pycloud_parallel.execution.support import (
    _get_local_ip,
    _prepare_code_blob,
    _prepare_managed_globals_values_for_upload,
    _prepare_task_payload_for_submit,
    _put_data_via_clients,
    _resolve_public_target_arg,
)
from pycloud_parallel.data.ref import DataRef
from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2


logger = logging.getLogger(__name__)
_TASK_POOL_CLOSE_RETRY_DELAYS_SEC = (0.0, 0.5, 1.0, 2.0)
_DEFAULT_MAX_IN_FLIGHT_WORKER_FACTOR = 1.5


def _infocenter_client(*args, **kwargs):
    from pycloud_parallel.controlplane.infocenter_client import InfoCenterClient

    return InfoCenterClient(*args, **kwargs)


def _node_control_client(*args, **kwargs):
    from pycloud_parallel.controlplane.node_control_client import NodeControlClient

    return NodeControlClient(*args, **kwargs)


def _resolve_task_results_data(batch: Any, results: Sequence[pb2.TaskResult]) -> List[Any]:
    return [batch.fetch_result_data(item) for item in results]


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


_LEGACY_TASKPOOL_UNORDERED_KWARGS = {
    "receive_batch",
    "submit_timeout_sec",
    "result_timeout_sec",
    "wait_ms",
    "raise_on_error",
    "node_window_factor",
}


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
        backend: Optional[Any] = None,
    ) -> None:
        self._pools = pools
        self.nodes = nodes
        self._backend = backend
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
        if self._backend is None:
            self._backend = NativeTaskBackend(self)
        self._init_execution_session_state()

    def _replica_handles(self) -> Dict[str, ExecutionReplicaHandle]:
        if self._backend is not None and hasattr(self._backend, "replicas"):
            return dict(getattr(self._backend, "replicas") or {})
        return self._pools

    @property
    def client_id(self) -> str:
        if self._backend is not None and hasattr(self._backend, "client_id"):
            return str(getattr(self._backend, "client_id"))
        first = next(iter(self._pools.values()))
        return first.owner_client_id

    @property
    def job_id(self) -> str:
        if self._backend is not None and hasattr(self._backend, "job_id"):
            return str(getattr(self._backend, "job_id"))
        return self._job_id

    @property
    def code_version(self) -> str:
        if self._backend is not None and hasattr(self._backend, "code_version"):
            return str(getattr(self._backend, "code_version"))
        first = next(iter(self._pools.values()))
        return first.code_version

    @property
    def node_ids(self) -> Sequence[str]:
        if self._backend is not None and hasattr(self._backend, "node_ids"):
            return list(getattr(self._backend, "node_ids"))
        return [self.nodes[key].node_id if key in self.nodes else key for key in self._pools.keys()]

    @property
    def node_instance_ids(self) -> Sequence[str]:
        if self._backend is not None and hasattr(self._backend, "node_instance_ids"):
            return list(getattr(self._backend, "node_instance_ids"))
        return list(self._pools.keys())

    @property
    def methods(self) -> List[str]:
        if self._backend is not None and hasattr(self._backend, "methods"):
            return list(getattr(self._backend, "methods"))
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
        if self._backend is not None and hasattr(self._backend, "replicas"):
            active = {str(node_id) for node_id in getattr(self._backend, "replicas").keys()}
            return [
                node_id
                for node_id in ordered_node_ids
                if node_id in active and self._pool_candidate_allowed(node_id)
            ]
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

    def _pool_before_probe(self, node_id: str) -> bool:
        return before_probe(self._pool_breaker_state(node_id))

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
            node = self.nodes.get(node_id)
            worker_capacity = max(0, int(getattr(pool, "worker_count", 0) or 0))
            alive_workers = max(
                0,
                int(getattr(node, "task_pool_worker_available", 0) or 0),
            )
            if alive_workers <= 0 and pool is not None:
                with contextlib.suppress(Exception):
                    info = pool.get_status()
                    alive_workers = max(0, int(getattr(info, "alive_workers", 0) or 0))
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

    def _select_pool_node(self, *, strategy: str = "taskpool_default") -> str:
        node_ids = self._available_pool_node_ids()
        if not node_ids:
            raise RuntimeError("task pool has no active node pools")
        candidates, state = self._build_pool_scheduler_candidates(allowed_node_ids=node_ids)
        profile = resolve_taskpool_strategy(strategy)
        with self._pool_lock:
            idx = self._pool_cycle
            self._pool_cycle += 1
        selected = select_one_candidate(
            candidates,
            profile=profile,
            state=state,
            round_robin_counter=idx,
        )
        return str(selected.id)

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
        if should_use_transport_payload_bytes(
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
        if self._backend is not None:
            return self._backend.submit_payloads(
                payloads,
                task_method=task_method,
                strategy=strategy,
                timeout_sec=timeout_sec,
                job_id=job_id,
                task_id_prefix=task_id_prefix,
                timeout_hint_sec=timeout_hint_sec,
                priority=priority,
                runtime_key=runtime_key,
                serialization_mode=serialization_mode,
            )
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
        profile = resolve_taskpool_strategy(strategy)
        temp_state = SchedulerState(
            local_inflight_by_candidate=dict(self._scheduler_state.local_inflight_by_candidate),
            disabled_candidates=set(self._scheduler_state.disabled_candidates),
            recent_submit_failures=dict(self._scheduler_state.recent_submit_failures),
        )
        grouped: Dict[str, List[pb2.TaskSubmitItem]] = {}
        for payload in payloads:
            candidates, state = self._build_pool_scheduler_candidates(
                allowed_node_ids=self._available_pool_node_ids(),
                state=temp_state,
            )
            if not candidates:
                raise RuntimeError("task pool has no active node pools")
            with self._pool_lock:
                rr = self._pool_cycle
                self._pool_cycle += 1
            selected = select_one_candidate(
                candidates,
                profile=profile,
                state=state,
                round_robin_counter=rr,
            )
            target_node_id = str(selected.id)
            temp_state.local_inflight_by_candidate[target_node_id] = (
                int(temp_state.local_inflight_by_candidate.get(target_node_id, 0) or 0) + 1
            )
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
    ) -> List[Dict[str, object]]:
        shared = dict(shared_kwargs or {})
        merged: List[Dict[str, object]] = []
        for payload in payloads:
            if not isinstance(payload, dict):
                raise TypeError("payloads must be mapping payloads")
            merged.append({**dict(payload), **shared})
        return merged

    def _item_with_index(self, item: ExecutionItem, *, index: int, key: Union[int, str]) -> ExecutionItem:
        return replace(item, index=int(index), key=key)

    def _submit_indexed_payloads(
        self,
        payloads: Sequence[Dict[str, object]],
        *,
        task_method: str = "",
    ) -> Tuple[Set[str], Dict[str, int], List[ExecutionItem]]:
        if self._closed:
            raise RuntimeError("task pool session is closed")
        normalized_method = str(task_method or self._task_method).strip() or self._task_method
        self._ensure_method(normalized_method)
        temp_state = SchedulerState(
            local_inflight_by_candidate=dict(self._scheduler_state.local_inflight_by_candidate),
            disabled_candidates=set(self._scheduler_state.disabled_candidates),
            recent_submit_failures=dict(self._scheduler_state.recent_submit_failures),
        )
        grouped: Dict[str, List[Tuple[int, pb2.TaskSubmitItem]]] = {}
        index_by_task_id: Dict[str, int] = {}
        for idx, payload in enumerate(payloads):
            candidates, state = self._build_pool_scheduler_candidates(
                allowed_node_ids=self._available_pool_node_ids(),
                state=temp_state,
            )
            if not candidates:
                raise RuntimeError("task pool has no active node pools")
            with self._pool_lock:
                rr = self._pool_cycle
                self._pool_cycle += 1
            selected = select_one_candidate(
                candidates,
                profile=TASKPOOL_DEFAULT,
                state=state,
                round_robin_counter=rr,
            )
            target_node_id = str(selected.id)
            temp_state.local_inflight_by_candidate[target_node_id] = (
                int(temp_state.local_inflight_by_candidate.get(target_node_id, 0) or 0) + 1
            )
            item = self._build_task_submit_item(
                node_id=target_node_id,
                payload=dict(payload or {}),
                timeout_hint_sec=0,
                priority=1,
            )
            grouped.setdefault(target_node_id, []).append((idx, item))
            index_by_task_id[str(item.task_id)] = idx

        accepted_task_ids: Set[str] = set()
        rejected_items: List[ExecutionItem] = []
        for node_id, entries in grouped.items():
            resp = self._submit_task_items_to_node(
                node_id,
                [item for _idx, item in entries],
                job_id=self.job_id,
            )
            accepted_ids = {str(item.task_id) for item in resp.accepted if str(item.task_id).strip()}
            accepted_task_ids.update(accepted_ids)
            for rejected in resp.rejected:
                task_id = str(rejected.task_id or "").strip()
                index = int(index_by_task_id.get(task_id, -1))
                rejected_items.append(
                    ExecutionItem(
                        index=index,
                        ok=False,
                        result=None,
                        error_type="TaskRejected",
                        error_message=str(rejected.message or "task rejected"),
                        node_id="",
                        key=task_id or index,
                        task_id=task_id,
                    )
                )
        return accepted_task_ids, index_by_task_id, rejected_items

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
        if self._backend is not None:
            yield from self._backend.iter_results(
                max_count=max_count,
                timeout_sec=timeout_sec,
                wait_ms=wait_ms,
                limit=limit,
                job_id=job_id,
            )
            return
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

    def update_globals(self, values: Dict[str, object]) -> str:
        if self._backend is not None:
            return str(self._backend.update_globals(values))
        if self._closed:
            raise RuntimeError("task pool session is closed")
        pools_snapshot = list(self._pools.items())
        active_clients = [pool._client for _node_id, pool in pools_snapshot]  # noqa: SLF001
        prepare_kwargs: Dict[str, object] = {}
        if str(self._serialization_mode or "").strip() and self._serialization_mode != "legacy_v1":
            prepare_kwargs["serialization_mode"] = self._serialization_mode
        prepared_values = _prepare_managed_globals_values_for_upload(
            active_clients,
            values,
            effective_policy=self.effective_policy,
            **prepare_kwargs,
        )
        digests: Dict[str, str] = {}
        failed_nodes: Dict[str, str] = {}
        for node_id, pool in pools_snapshot:
            try:
                resp = pool._client.update_runtime_globals_prepared(  # noqa: SLF001
                    client_id=pool.pool_id,
                    code_version=pool.code_version,
                    runtime_key=pool.pool_id,
                    code_token=pool.pool_token,
                    prepared_values=prepared_values,
                )
                digests[node_id] = resp.globals_digest
            except Exception as exc:
                failed_nodes[node_id] = repr(exc)

        if not digests:
            raise RuntimeError(f"update_globals failed on all nodes: {failed_nodes}")
        self.globals_digests = dict(digests)
        unique = {digest for digest in digests.values() if str(digest).strip()}
        return next(iter(unique), "") if len(unique) == 1 else next(iter(digests.values()))

    def _iter_received_items(
        self,
        *,
        max_count: Optional[int] = None,
        timeout_sec: float = 30.0,
        wait_ms: int = 500,
        limit: int = 100,
        job_id: str = "",
        task_ids: Optional[Set[str]] = None,
    ) -> Iterator[ExecutionItem]:
        for node_id, task_result in self._iter_result_items(
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
            yield from self._iter_received_items(
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
        for index, result in self.imap_unordered(
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
        ):
            if result is None:
                yield ExecutionItem(
                    index=int(index),
                    ok=False,
                    result=None,
                    error_type="TaskFailed",
                    error_message="task failed",
                    key=int(index),
                )
            else:
                yield ExecutionItem(
                    index=int(index),
                    ok=True,
                    result=result,
                    key=int(index),
                )

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
        if self._backend is not None:
            return self._backend.wait_for_results(
                expected_count=expected_count,
                timeout_sec=timeout_sec,
                wait_ms=wait_ms,
                limit=limit,
                job_id=job_id,
            )
        self._assert_session_available("wait_for_results")
        max_count = max(0, int(expected_count or 0))
        return [
            item
            for _node_id, item in self._iter_result_items(
                max_count=(max_count if max_count > 0 else None),
                timeout_sec=timeout_sec,
                wait_ms=wait_ms,
                limit=limit,
                job_id=job_id,
            )
        ]

    def wait_for_data(
        self,
        *,
        expected_count: int = 0,
        timeout_sec: float = 30.0,
    ) -> Sequence[Any]:
        if self._backend is not None:
            return self._backend.wait_for_data(expected_count=expected_count, timeout_sec=timeout_sec)
        self._assert_session_available("wait_for_data")
        results = self.wait_for_results(expected_count=expected_count, timeout_sec=timeout_sec)
        return _resolve_task_results_data(
            _NativePoolResultAdapter(serialization_mode=self._serialization_mode),
            results,
        )

    def submit_values(
        self,
        values: Sequence[Any],
        *,
        arg_name: str = "value",
        task_method: str = "",
        strategy: str = "taskpool_default",
        serialization_mode: str = "",
        **shared_kwargs,
    ) -> pb2.SubmitTasksResponse:
        if self._backend is not None:
            return self._backend.submit_values(
                values,
                arg_name=arg_name,
                task_method=task_method,
                strategy=strategy,
                serialization_mode=serialization_mode,
                **shared_kwargs,
            )
        normalized_arg = str(arg_name or "value").strip() or "value"
        payloads = [{normalized_arg: value, **dict(shared_kwargs)} for value in values]
        return self.submit_payloads(
            payloads,
            task_method=task_method,
            strategy=strategy,
            serialization_mode=serialization_mode,
        )

    def imap_unordered(
        self,
        payloads: Iterable[Dict[str, object]],
        *,
        task_method: str = "",
        strategy: str = "taskpool_default",
        max_in_flight: Optional[int] = None,
        timeout_sec: float = 30.0,
        **shared_kwargs,
    ) -> Iterator[Tuple[int, Any]]:
        if self._backend is not None:
            yield from self._backend.imap_unordered(
                payloads,
                task_method=task_method,
                strategy=strategy,
                max_in_flight=max_in_flight,
                timeout_sec=timeout_sec,
                **shared_kwargs,
            )
            return
        if self._closed:
            raise RuntimeError("task pool session is closed")
        self._enter_exclusive_mode("imap_unordered", require_clean=True)
        try:
            self._ensure_method(str(task_method or self._task_method).strip() or self._task_method)
            max_pending = self._resolve_max_in_flight(max_in_flight)
            max_receive = max(1, int(shared_kwargs.pop("receive_batch", 1) or 1))
            submit_timeout_sec = float(shared_kwargs.pop("submit_timeout_sec", timeout_sec) or timeout_sec)
            result_timeout_sec = float(shared_kwargs.pop("result_timeout_sec", timeout_sec) or timeout_sec)
            wait_ms = int(shared_kwargs.pop("wait_ms", 500) or 500)
            raise_on_error = bool(shared_kwargs.pop("raise_on_error", True))
            _node_window_factor = float(shared_kwargs.pop("node_window_factor", 2.0) or 2.0)
            if shared_kwargs:
                unexpected = ", ".join(sorted(shared_kwargs))
                raise TypeError(f"unexpected keyword arguments for imap_unordered(): {unexpected}")
            profile = resolve_taskpool_strategy(strategy)
            payload_iter = iter(payloads)
            retry_payloads: "deque[Tuple[int, Dict[str, object]]]" = deque()
            input_exhausted = False
            ready_items: "deque[ExecutionItem]" = deque()
            next_payload_index = 0
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

            def _plan_targets(
                available_by_node: Dict[str, int],
                *,
                node_order: Sequence[str],
                max_new_tasks: int,
            ) -> List[str]:
                if max_new_tasks <= 0:
                    return []
                remaining = {
                    node_id: max(0, int(available_by_node.get(node_id, 0) or 0))
                    for node_id in node_order
                    if node_id not in disabled_submit_nodes
                }
                planned: List[str] = []
                rr_counter = poll_start_idx
                while max_new_tasks > 0:
                    allowed = [node_id for node_id in node_order if remaining.get(node_id, 0) > 0]
                    if not allowed:
                        break
                    candidates, state = self._build_pool_scheduler_candidates(
                        allowed_node_ids=allowed,
                        state=SchedulerState(
                            local_inflight_by_candidate=dict(inflight_by_node),
                            disabled_candidates=set(disabled_submit_nodes),
                            recent_submit_failures=dict(infra_failures_by_node),
                        ),
                    )
                    selected = select_one_candidate(
                        candidates,
                        profile=profile,
                        state=state,
                        round_robin_counter=rr_counter,
                    )
                    node_id = str(selected.id)
                    rr_counter += 1
                    planned.append(node_id)
                    remaining[node_id] = max(0, int(remaining.get(node_id, 0) or 0) - 1)
                    max_new_tasks -= 1
                return planned

            def _next_payload() -> Optional[Tuple[int, Dict[str, object]]]:
                nonlocal input_exhausted, next_payload_index
                if retry_payloads:
                    index, payload = retry_payloads.popleft()
                    return int(index), dict(payload or {})
                if input_exhausted:
                    return None
                try:
                    payload = dict(next(payload_iter) or {})
                    index = next_payload_index
                    next_payload_index += 1
                    return index, payload
                except StopIteration:
                    input_exhausted = True
                    return None

            def _requeue_payloads_front(items: Sequence[Tuple[int, Dict[str, object], pb2.TaskSubmitItem]]) -> None:
                for index, payload, _item in reversed(list(items)):
                    retry_payloads.appendleft((int(index), dict(payload or {})))

            def _fill_from_quota(
                available_by_node: Dict[str, int],
                *,
                node_order: Sequence[str],
            ) -> int:
                available_global = max(0, max_pending - sum(inflight_by_node.values()))
                if available_global <= 0:
                    return 0
                capped_by_node = {
                    node_id: max(0, int(available_by_node.get(node_id, 0) or 0))
                    for node_id in node_order
                    if node_id not in disabled_submit_nodes
                }
                targets = _plan_targets(capped_by_node, node_order=node_order, max_new_tasks=available_global)
                if not targets:
                    return 0
                grouped: Dict[str, List[Tuple[int, Dict[str, object], pb2.TaskSubmitItem]]] = {}
                for node_id in targets:
                    indexed_payload = _next_payload()
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
                submitted = 0
                for node_id in node_order:
                    entries = grouped.get(node_id, [])
                    if not entries:
                        continue
                    try:
                        resp = self._submit_task_items_to_node(
                            node_id,
                            [item for _index, _payload, item in entries],
                            job_id=self.job_id,
                        )
                    except Exception as exc:
                        disabled_submit_nodes.add(node_id)
                        self._mark_pool_submit_failure(node_id, failure_kind=SUBMIT_FAILED, error=exc)
                        scheduler_failures[node_id] = repr(exc)
                        _requeue_payloads_front(entries)
                        continue
                    accepted_ids = {str(item.task_id) for item in resp.accepted if str(item.task_id).strip()}
                    if not accepted_ids:
                        self._mark_pool_submit_failure(
                            node_id,
                            failure_kind=SUBMIT_FAILED,
                            error=RuntimeError("submit returned no accepted task ids"),
                        )
                        _requeue_payloads_front(entries)
                        continue
                    self._mark_pool_submit_success(node_id)
                    accepted_count = 0
                    rejected_entries: List[Tuple[int, Dict[str, object], pb2.TaskSubmitItem]] = []
                    for entry in entries:
                        if str(entry[2].task_id) in accepted_ids:
                            accepted_count += 1
                        else:
                            rejected_entries.append(entry)
                    if rejected_entries:
                        _requeue_payloads_front(rejected_entries)
                    inflight_by_node[node_id] += accepted_count
                    submitted += accepted_count
                return submitted

            initial_quota = {node_id: max_pending for node_id in node_ids}
            _fill_from_quota(initial_quota, node_order=node_ids)
            if self._pending_result_count() <= 0 and not retry_payloads and input_exhausted:
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
                        yield item.index, None
                    else:
                        yield item.index, item.data
                    yielded += 1
                if yielded > 0:
                    continue

                if input_exhausted and not retry_payloads and self._pending_result_count() <= 0:
                    return

                if self._pending_result_count() <= 0:
                    idle_quota = {node_id: max_pending for node_id in node_ids}
                    submitted_now = _fill_from_quota(idle_quota, node_order=node_ids)
                    if submitted_now > 0:
                        wait_deadline = time.time() + max(0.1, float(result_timeout_sec))
                        continue
                    if retry_payloads or not input_exhausted:
                        failure_suffix = f"; failures={scheduler_failures}" if scheduler_failures else ""
                        raise RuntimeError(f"imap_unordered could not submit tasks to any active task pool node{failure_suffix}")
                    return

                ordered_node_ids = node_ids[poll_start_idx:] + node_ids[:poll_start_idx]
                poll_start_idx = (poll_start_idx + 1) % len(node_ids)
                completed_items: List[ExecutionItem] = []
                freed_by_node: Dict[str, int] = {}
                completion_order: List[str] = []
                for node_id in ordered_node_ids:
                    pull_limit = max(1, int(inflight_by_node.get(node_id, 0) or 1))
                    try:
                        resp = self._pools[node_id].pull_results(limit=pull_limit, wait_ms=0, cursor="")
                    except Exception as exc:
                        disabled_submit_nodes.add(node_id)
                        self._mark_pool_submit_failure(node_id, failure_kind=SUBMIT_FAILED, error=exc)
                        scheduler_failures[node_id] = repr(exc)
                        continue
                    if not resp.results:
                        continue
                    for result in resp.results:
                        normalized = str(result.task_id or "").strip()
                        if not self._is_pending_task_id(normalized):
                            continue
                        self._mark_result_consumed(result.task_id)
                        inflight_by_node[node_id] = max(0, inflight_by_node[node_id] - 1)
                        freed_by_node[node_id] = freed_by_node.get(node_id, 0) + 1
                        if node_id not in completion_order:
                            completion_order.append(node_id)
                        if int(result.status) == int(pb2.TASK_STATUS_FAILED_INFRA):
                            infra_failures_by_node[node_id] = infra_failures_by_node.get(node_id, 0) + 1
                            self._mark_pool_submit_failure(
                                node_id,
                                failure_kind=REMOTE_INFRA_FAILED,
                                error=RuntimeError(str(result.error.message or "remote infra failure")),
                            )
                            if infra_failures_by_node[node_id] >= 2:
                                disabled_submit_nodes.add(node_id)
                        elif int(result.status) == int(pb2.TASK_STATUS_SUCCEEDED):
                            infra_failures_by_node.pop(node_id, None)
                            self._mark_pool_submit_success(node_id)
                        item = self._task_result_to_item(node_id, result)
                        index = int(task_index_by_id.get(normalized, -1))
                        completed_items.append(self._item_with_index(item, index=index, key=index if index >= 0 else normalized))

                if completed_items:
                    wait_deadline = time.time() + max(0.1, float(result_timeout_sec))
                    ready_items.extend(completed_items)
                    if not (raise_on_error and any(not item.ok for item in completed_items)):
                        freed_total = max(0, sum(int(value or 0) for value in freed_by_node.values()))
                        if freed_total > 0:
                            refill_quota = {node_id: freed_total for node_id in node_ids}
                            _fill_from_quota(refill_quota, node_order=node_ids)
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
        **shared_kwargs,
    ) -> Iterator[Tuple[int, Any]]:
        """Yield ``(index, result_or_none)`` in completion order for a submitted batch."""
        forbidden = sorted(_LEGACY_TASKPOOL_UNORDERED_KWARGS.intersection(shared_kwargs))
        if forbidden:
            raise TypeError(
                f"TaskPool.unordered() no longer accepts low-level control args: {', '.join(forbidden)}; "
                "use TaskPool.imap_unordered() for low-level streaming controls"
            )
        if self._backend is not None:
            yield from self._backend.unordered(
                payloads,
                task_method=task_method,
                strategy=strategy,
                max_in_flight=max_in_flight,
                timeout_sec=timeout_sec,
                **shared_kwargs,
            )
            return
        for item in self.iter_items(
            payloads,
            task_method=task_method,
            strategy=strategy,
            max_in_flight=max_in_flight,
            timeout_sec=timeout_sec,
            **shared_kwargs,
        ):
            yield item.index, item.result if item.ok else None

    async def aunordered(
        self,
        payloads: Iterable[Dict[str, object]],
        *,
        task_method: str = "",
        strategy: str = "taskpool_default",
        max_in_flight: Optional[int] = None,
        timeout_sec: float = 30.0,
        **shared_kwargs,
    ) -> AsyncIterator[Tuple[int, Any]]:
        """Async counterpart of :meth:`unordered` with the same return shape."""
        forbidden = sorted(_LEGACY_TASKPOOL_UNORDERED_KWARGS.intersection(shared_kwargs))
        if forbidden:
            raise TypeError(
                f"TaskPool.aunordered() no longer accepts low-level control args: {', '.join(forbidden)}; "
                "use TaskPool.imap_unordered() for low-level streaming controls"
            )
        async for item in self.aiter_items(
            payloads,
            task_method=task_method,
            strategy=strategy,
            max_in_flight=max_in_flight,
            timeout_sec=timeout_sec,
            **shared_kwargs,
        ):
            yield item.index, item.result if item.ok else None

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
        **shared_kwargs,
    ) -> int:
        if self._backend is not None:
            return int(
                self._backend.consume_unordered(
                    payloads,
                    handle=handle,
                    task_method=task_method,
                    strategy=strategy,
                    max_in_flight=max_in_flight,
                    timeout_sec=timeout_sec,
                    receive_batch=receive_batch,
                    submit_timeout_sec=submit_timeout_sec,
                    result_timeout_sec=result_timeout_sec,
                    wait_ms=wait_ms,
                    raise_on_error=raise_on_error,
                    node_window_factor=node_window_factor,
                    **shared_kwargs,
                )
            )
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
        ):
            index: Union[int, str] = task_id
            if isinstance(task_id, str) and task_id.rsplit("-", 1)[-1].isdigit():
                index = max(0, int(task_id.rsplit("-", 1)[-1]) - 1)
            handle(index, result)
            processed += 1
        return processed

    def map(
        self,
        values: Sequence[Any],
        *,
        arg_name: str = "value",
        task_method: str = "",
        strategy: str = "taskpool_default",
        timeout_sec: float = 30.0,
        **shared_kwargs,
    ) -> Sequence[Any]:
        if self._backend is not None:
            return self._backend.map(
                values,
                arg_name=arg_name,
                task_method=task_method,
                strategy=strategy,
                timeout_sec=timeout_sec,
                **shared_kwargs,
            )
        payloads = [{str(arg_name or "value").strip() or "value": value, **dict(shared_kwargs)} for value in values]
        items = self.collect_items(
            payloads,
            task_method=task_method,
            strategy=strategy,
            max_in_flight=max(1, len(payloads) or 1),
            timeout_sec=timeout_sec,
        )
        return [item.result if item.ok else None for item in items]

    async def amap(
        self,
        values: Sequence[Any],
        *,
        arg_name: str = "value",
        task_method: str = "",
        strategy: str = "taskpool_default",
        timeout_sec: float = 30.0,
        **shared_kwargs,
    ) -> Sequence[Any]:
        return await asyncio.to_thread(
            lambda: self.map(
                values,
                arg_name=arg_name,
                task_method=task_method,
                strategy=strategy,
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
        if self._backend is not None:
            return self._backend.cancel_job(reason=reason, job_id=job_id)
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
        if self._backend is not None:
            return self._backend.status_map()
        return {node_id: pool.get_status() for node_id, pool in self._pools.items()}

    def status(self) -> ExecutionSessionStatus:
        return super().status()

    def is_alive(self) -> bool:
        if self._backend is not None and hasattr(self._backend, "is_alive"):
            return bool(self._backend.is_alive())
        return super().is_alive()

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
        if self._backend is not None and not isinstance(self._backend, NativeTaskBackend):
            self._backend.close()
            return
        for pool in self._pools.values():
            _close_task_pool_replica(pool, reason="task pool session close")
            with contextlib.suppress(Exception):
                pool._client.close()  # noqa: SLF001

    def __enter__(self) -> "_TaskPoolSessionBase":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def __getattr__(self, name: str):
        if self._backend is not None and hasattr(self._backend, "__getattr__") and not name.startswith("_"):
            return self._backend.__getattr__(name)
        if name.startswith("_"):
            raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")
        return _TaskPoolCallProxy(session=self, method_name=self._ensure_method(name))

    def call_sync(self, method: str, **kwargs) -> Any:
        if self._backend is not None:
            return self._backend.call_sync(method, **kwargs)
        normalized = self._ensure_method(method)
        return getattr(self, normalized).sync(**kwargs)

    async def call(self, method: str, **kwargs) -> Any:
        if self._backend is not None:
            return await self._backend.call(method, **kwargs)
        normalized = self._ensure_method(method)
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, lambda: getattr(self, normalized).sync(**kwargs))

    def __repr__(self) -> str:
        effective_policy_text = ""
        if self.effective_policy is not None:
            effective_policy_text = (
                f" effective_policy={self.effective_policy.policy_id}@v{self.effective_policy.version}"
            )
        if isinstance(self._backend, NativeTaskBackend) or self._backend is None:
            return (
                f"<{type(self).__name__} methods={self.methods} "
                f"nodes={len(self.node_ids)} serialization_mode={self._serialization_mode}"
                f"{effective_policy_text}>"
            )
        if self._backend is not None:
            return repr(self._backend)
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
    entry_func: Optional[Callable] = None,
    func: Optional[Callable] = None,
    artifact_path: Union[str, os.PathLike[str], Sequence[Union[str, os.PathLike[str]]]] = "",
    blob: Optional[bytes] = None,
    runtime: str = "py3",
    entry_module: Any = "",
    entry_callable: Any = "run",
    package_format: str = "",
    dependency_allowlist: Optional[Sequence[str]] = None,
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
    if (
        module_source is None
        and inspect.ismodule(entry_module)
        and source is None
        and artifact is None
        and entry_func is None
        and func is None
        and not artifact_path
        and blob is None
    ):
        module_source = entry_module
    normalized_resource_paths = [item for item in list(resource_paths or ()) if str(item or "").strip()]
    if normalized_resource_paths and module_source is None:
        raise ValueError("resource_paths requires a module source")
    if normalized_resource_paths and module_source is not None:
        module_blob, module_filename = _prepare_code_blob(module=module_source, resource_paths=normalized_resource_paths)
        source = module_blob
        entry_module = _default_entry_module_for_module(module_source)
        package_format = _resolve_package_format(package_format, module_filename, default="py")

    source_func = entry_func if entry_func is not None else func
    normalized_artifact = _normalize_artifact_input(
        consumer_kind="task",
        source=source,
        artifact=artifact,
        deps=deps,
        func=source_func,
        artifact_path=artifact_path,
        blob=blob,
        runtime=runtime,
        entry_module=entry_module,
        entry_callable=entry_callable,
        package_format=package_format,
        dependency_allowlist=dependency_allowlist,
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
    fetch_limit = requested_count if requested_count > 0 else node_limit
    with _infocenter_client(infocenter_target, timeout_sec=timeout_sec) as infocenter:
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
    effective_pool_name = str(pool_name or f"task-pool-{uuid.uuid4().hex[:10]}").strip()
    effective_policy = resolve_effective_policy(
        get_policy_profile(policy_id or get_default_policy_id_for_binding("taskpool_default")),
        requested_mode=serialization_mode,
        context="taskpool_session",
    )

    pools: Dict[str, NativeTaskPoolClient] = {}
    nodes: Dict[str, InfoCenterNode] = {}
    for node in desired_nodes:
        client = _node_control_client(node.control_addr, timeout_sec=timeout_sec)
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
    session._start_keepalive()
    return session


class _NativePoolResultAdapter:
    def __init__(self, *, serialization_mode: str = "") -> None:
        self.serialization_mode = resolve_effective_serialization_mode(
            request_mode=serialization_mode,
            context="taskpool_session",
        )

    def fetch_result_data(self, task_result: pb2.TaskResult, *, target_path: str = ""):
        del target_path
        if task_result.HasField("transport_result") and str(task_result.transport_result.codec or "").strip():
            return decode_transport_payload_bytes(
                str(task_result.transport_result.codec or ""),
                int(task_result.transport_result.version or 0),
                task_result.transport_result.payload,
                context="taskpool_session",
            )
        if task_result.result:
            raw = struct_to_python(task_result.result)
            return decode_result_from_transport(
                raw,
                mode=detect_transport_mode(raw, default=self.serialization_mode or "legacy_v1"),
                context="taskpool_session",
            )
        raise RuntimeError(task_result.error.message or "task failed")



__all__ = [
    "_NativePoolResultAdapter",
    "TaskPool",
]


class TaskPool(_TaskPoolSessionBase):
    """V1 task-pool session."""

    @classmethod
    def open(
        cls,
        *,
        target: str = "",
        **kwargs: Any,
    ) -> "TaskPool":
        """Product-facing open action for V1 task pools.

        Default path: ``TaskPool.open(target=\"127.0.0.1:50051\", source=my_module, ...)``.
        Advanced path: ``TaskPool.open(artifact=Artifact(...), ...)``.
        """
        effective_target = _resolve_public_target_arg(
            target=target,
            kwargs=kwargs,
            action_name="TaskPool.open()",
        )
        return cls.from_infocenter(infocenter_target=effective_target, **kwargs)

    @classmethod
    def from_infocenter(
        cls,
        *,
        infocenter_target: str,
        job_id: str = "",
        source: Any = None,
        owner_client_id: Optional[str] = None,
        pool_name: Optional[str] = None,
        artifact: Optional[Any] = None,
        deps: Optional[Any] = None,
        entry_func: Optional[Callable] = None,
        func: Optional[Callable] = None,
        artifact_path: Union[str, os.PathLike[str], Sequence[Union[str, os.PathLike[str]]]] = "",
        blob: Optional[bytes] = None,
        runtime: str = "py3",
        entry_module: Any = "",
        entry_callable: Any = "run",
        package_format: str = "",
        dependency_allowlist: Optional[Sequence[str]] = None,
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
            entry_func=entry_func,
            func=func,
            artifact_path=artifact_path,
            blob=blob,
            runtime=runtime,
            entry_module=entry_module,
            entry_callable=entry_callable,
            package_format=package_format,
            dependency_allowlist=dependency_allowlist,
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
