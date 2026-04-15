from __future__ import annotations

"""Task-session facade extracted from controlplane client."""

import asyncio
from collections import deque
import contextlib
import math
import os
import threading
import time
from typing import Any, Callable, Dict, Iterable, Iterator, List, Optional, Sequence, Set, Tuple, Union
import uuid

from pycloud_parallel.controlplane import client as client_mod
from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2
from pycloud_parallel.controlplane.session_handle import ExecutionReplicaHandle


def _forward(name: str):
    return lambda *args, **kwargs: getattr(client_mod, name)(*args, **kwargs)


TaskExecutionSession = client_mod.TaskExecutionSession
NativeTaskBackend = client_mod.NativeTaskBackend
_TaskPoolCallProxy = client_mod._TaskPoolCallProxy
_ServiceCompatTaskBackend = client_mod._ServiceCompatTaskBackend
TaskPoolItem = client_mod.TaskPoolItem
OBJECT_CHUNK_SIZE_BYTES = client_mod.OBJECT_CHUNK_SIZE_BYTES
serialize_inline_payload = client_mod.serialize_inline_payload
struct_to_dict = client_mod.struct_to_dict

InfoCenterClient = _forward("InfoCenterClient")
NodeControlClient = _forward("NodeControlClient")
_prepare_managed_globals_values_for_upload = _forward("_prepare_managed_globals_values_for_upload")
_prepare_task_payload_for_submit = _forward("_prepare_task_payload_for_submit")
_normalize_artifact_input = _forward("_normalize_artifact_input")
_prepare_artifact = _forward("_prepare_artifact")
_get_local_ip = _forward("_get_local_ip")
_node_instance_key_from_node = _forward("_node_instance_key_from_node")


def _resolve_task_results_data(batch: Any, results: Sequence[pb2.TaskResult]) -> List[Any]:
    return [batch.fetch_result_data(item) for item in results]


class TaskPoolSession(TaskExecutionSession):
    """Native dedicated task pool session backed by NodeControl task pool RPCs."""

    def __init__(
        self,
        *,
        pools: Dict[str, NativeTaskPoolClient],
        nodes: Dict[str, InfoCenterNode],
        task_method: str,
        job_id: str = "",
        backend: Optional[Any] = None,
    ) -> None:
        self._pools = pools
        self.nodes = nodes
        self._backend = backend
        self._task_method = str(task_method or "run").strip() or "run"
        self._job_id = str(job_id or f"pool-{uuid.uuid4().hex[:12]}").strip()
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

    def _build_task_submit_item(
        self,
        *,
        node_id: str,
        payload: Dict[str, object],
        task_id_prefix: str = "",
        timeout_hint_sec: int = 0,
        priority: int = 1,
    ) -> pb2.TaskSubmitItem:
        task_id = self._next_task_id()
        prefix = str(task_id_prefix or f"{self.job_id}-task").strip()
        if prefix:
            task_id = f"{prefix}-{task_id.rsplit('-', 1)[-1]}"
        prepared_payload = _prepare_task_payload_for_submit(
            self._pools[node_id]._client,  # noqa: SLF001
            dict(payload or {}),
        )
        _, payload_struct, _ = serialize_inline_payload(prepared_payload, context="task pool payload")
        return pb2.TaskSubmitItem(
            task_id=task_id,
            payload=payload_struct,
            timeout_hint_sec=max(0, int(timeout_hint_sec)),
            priority=max(1, int(priority)),
        )

    def _register_pending_task_ids(self, accepted: Sequence[pb2.TaskAccepted]) -> None:
        with self._result_state_lock:
            self._pending_task_ids.update(str(item.task_id) for item in accepted if str(item.task_id).strip())

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
        self._register_pending_task_ids(resp.accepted)
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
        timeout_sec: float = 60.0,
        job_id: str = "",
        task_id_prefix: str = "",
        timeout_hint_sec: int = 0,
        priority: int = 1,
        runtime_key: str = "",
    ) -> pb2.SubmitTasksResponse:
        if self._backend is not None:
            return self._backend.submit_payloads(
                payloads,
                task_method=task_method,
                timeout_sec=timeout_sec,
                job_id=job_id,
                task_id_prefix=task_id_prefix,
                timeout_hint_sec=timeout_hint_sec,
                priority=priority,
                runtime_key=runtime_key,
            )
        del timeout_sec, runtime_key
        self._assert_session_available("submit_payloads")
        if self._closed:
            raise RuntimeError("task pool session is closed")
        self._ensure_method(str(task_method or self._task_method).strip() or self._task_method)
        grouped: Dict[str, List[pb2.TaskSubmitItem]] = {}
        for payload in payloads:
            target_node_id = self._select_pool_node()
            grouped.setdefault(target_node_id, []).append(
                self._build_task_submit_item(
                    node_id=target_node_id,
                    payload=dict(payload or {}),
                    task_id_prefix=task_id_prefix,
                    timeout_hint_sec=max(0, int(timeout_hint_sec)),
                    priority=max(1, int(priority)),
                )
            )
        return self._submit_grouped_task_items(grouped, job_id=job_id)

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

    def _task_result_to_item(self, node_id: str, task_result: pb2.TaskResult) -> TaskPoolItem:
        resolved_node_id = str(self.nodes.get(node_id).node_id if node_id in self.nodes else node_id)
        if int(task_result.status) != int(pb2.TASK_STATUS_SUCCEEDED):
            error = task_result.error
            return TaskPoolItem(
                task_id=str(task_result.task_id or ""),
                node_id=resolved_node_id,
                node_instance_id=str(node_id),
                ok=False,
                status=int(task_result.status),
                data=None,
                error_type=str(error.type or ""),
                error_message=str(error.message or f"task failed: {task_result.task_id}"),
            )
        try:
            data = self._pools[node_id]._client.fetch_result_data(task_result)  # noqa: SLF001
            return TaskPoolItem(
                task_id=str(task_result.task_id or ""),
                node_id=resolved_node_id,
                node_instance_id=str(node_id),
                ok=True,
                status=int(task_result.status),
                data=data,
            )
        except Exception as exc:
            return TaskPoolItem(
                task_id=str(task_result.task_id or ""),
                node_id=resolved_node_id,
                node_instance_id=str(node_id),
                ok=False,
                status=int(task_result.status),
                data=None,
                error_type=exc.__class__.__name__,
                error_message=str(exc),
            )

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
        prepared_values = _prepare_managed_globals_values_for_upload(active_clients, values)
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
            yield self._task_result_to_item(node_id, task_result)

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
        if self._backend is not None:
            return self._backend.wait_for_results(
                expected_count=expected_count,
                timeout_sec=timeout_sec,
                wait_ms=wait_ms,
                limit=limit,
                job_id=job_id,
            )
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
        if self._backend is not None:
            return self._backend.wait_for_data(expected_count=expected_count, timeout_sec=timeout_sec)
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
        if self._backend is not None:
            return self._backend.submit_values(values, arg_name=arg_name, task_method=task_method, **shared_kwargs)
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
        node_window_factor: float = 2.0,
    ) -> Iterator[Tuple[str, Any]]:
        if self._backend is not None:
            yield from self._backend.imap_unordered(
                payloads,
                task_method=task_method,
                max_in_flight=max_in_flight,
                receive_batch=receive_batch,
                submit_timeout_sec=submit_timeout_sec,
                result_timeout_sec=result_timeout_sec,
                wait_ms=wait_ms,
                raise_on_error=raise_on_error,
                node_window_factor=node_window_factor,
            )
            return
        if self._closed:
            raise RuntimeError("task pool session is closed")
        self._enter_exclusive_mode("imap_unordered", require_clean=True)
        try:
            self._ensure_method(str(task_method or self._task_method).strip() or self._task_method)
            max_pending = max(1, int(max_in_flight or 1))
            max_receive = max(1, int(receive_batch or 1))
            window_factor = max(0.1, float(node_window_factor or 0.0))
            payload_iter = iter(payloads)
            retry_payloads: "deque[Dict[str, object]]" = deque()
            input_exhausted = False
            ready_items: "deque[TaskPoolItem]" = deque()
            node_ids = list(self._pools.keys())
            if not node_ids:
                raise RuntimeError("task pool has no node pools")
            window_by_node = {
                node_id: max(
                    1,
                    int(
                        math.ceil(
                            max(1, int(getattr(self._pools[node_id], "worker_count", 1) or 1)) * window_factor
                        )
                    ),
                )
                for node_id in node_ids
            }
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
                while max_new_tasks > 0:
                    progressed = False
                    for node_id in node_order:
                        if max_new_tasks <= 0:
                            break
                        if remaining.get(node_id, 0) <= 0:
                            continue
                        planned.append(node_id)
                        remaining[node_id] -= 1
                        max_new_tasks -= 1
                        progressed = True
                    if not progressed:
                        break
                return planned

            def _next_payload() -> Optional[Dict[str, object]]:
                nonlocal input_exhausted
                if retry_payloads:
                    return dict(retry_payloads.popleft() or {})
                if input_exhausted:
                    return None
                try:
                    return dict(next(payload_iter) or {})
                except StopIteration:
                    input_exhausted = True
                    return None

            def _requeue_payloads_front(items: Sequence[Tuple[Dict[str, object], pb2.TaskSubmitItem]]) -> None:
                for payload, _item in reversed(list(items)):
                    retry_payloads.appendleft(dict(payload or {}))

            def _fill_from_quota(
                available_by_node: Dict[str, int],
                *,
                node_order: Sequence[str],
            ) -> int:
                available_global = max(0, max_pending - sum(inflight_by_node.values()))
                if available_global <= 0:
                    return 0
                capped_by_node = {
                    node_id: min(
                        max(0, int(available_by_node.get(node_id, 0) or 0)),
                        max(0, int(window_by_node.get(node_id, 0) or 0) - int(inflight_by_node.get(node_id, 0) or 0)),
                    )
                    for node_id in node_order
                    if node_id not in disabled_submit_nodes
                }
                targets = _plan_targets(capped_by_node, node_order=node_order, max_new_tasks=available_global)
                if not targets:
                    return 0
                grouped: Dict[str, List[Tuple[Dict[str, object], pb2.TaskSubmitItem]]] = {}
                for node_id in targets:
                    payload = _next_payload()
                    if payload is None:
                        break
                    item = self._build_task_submit_item(
                        node_id=node_id,
                        payload=payload,
                        timeout_hint_sec=0,
                        priority=1,
                    )
                    grouped.setdefault(node_id, []).append((payload, item))
                submitted = 0
                for node_id in node_order:
                    entries = grouped.get(node_id, [])
                    if not entries:
                        continue
                    try:
                        resp = self._submit_task_items_to_node(
                            node_id,
                            [item for _payload, item in entries],
                            job_id=self.job_id,
                        )
                    except Exception as exc:
                        disabled_submit_nodes.add(node_id)
                        scheduler_failures[node_id] = repr(exc)
                        _requeue_payloads_front(entries)
                        continue
                    accepted_ids = {str(item.task_id) for item in resp.accepted if str(item.task_id).strip()}
                    if not accepted_ids:
                        _requeue_payloads_front(entries)
                        continue
                    accepted_count = 0
                    rejected_entries: List[Tuple[Dict[str, object], pb2.TaskSubmitItem]] = []
                    for entry in entries:
                        if str(entry[1].task_id) in accepted_ids:
                            accepted_count += 1
                        else:
                            rejected_entries.append(entry)
                    if rejected_entries:
                        _requeue_payloads_front(rejected_entries)
                    inflight_by_node[node_id] += accepted_count
                    submitted += accepted_count
                return submitted

            initial_quota = {node_id: window_by_node[node_id] for node_id in node_ids}
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
                        yield item.task_id, None
                    else:
                        yield item.task_id, item.data
                    yielded += 1
                if yielded > 0:
                    continue

                if input_exhausted and not retry_payloads and self._pending_result_count() <= 0:
                    return

                if self._pending_result_count() <= 0:
                    idle_quota = {
                        node_id: max(0, int(window_by_node.get(node_id, 0) or 0) - int(inflight_by_node.get(node_id, 0) or 0))
                        for node_id in node_ids
                    }
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
                completed_items: List[TaskPoolItem] = []
                freed_by_node: Dict[str, int] = {}
                completion_order: List[str] = []
                for node_id in ordered_node_ids:
                    pull_limit = max(1, int(inflight_by_node.get(node_id, 0) or 1))
                    try:
                        resp = self._pools[node_id].pull_results(limit=pull_limit, wait_ms=0, cursor="")
                    except Exception as exc:
                        disabled_submit_nodes.add(node_id)
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
                            if infra_failures_by_node[node_id] >= 2:
                                disabled_submit_nodes.add(node_id)
                        elif int(result.status) == int(pb2.TASK_STATUS_SUCCEEDED):
                            infra_failures_by_node.pop(node_id, None)
                        completed_items.append(self._task_result_to_item(node_id, result))

                if completed_items:
                    wait_deadline = time.time() + max(0.1, float(result_timeout_sec))
                    ready_items.extend(completed_items)
                    if not (raise_on_error and any(not item.ok for item in completed_items)):
                        refill_quota = {
                            node_id: min(
                                int(freed_by_node.get(node_id, 0) or 0),
                                max(
                                    0,
                                    int(window_by_node.get(node_id, 0) or 0)
                                    - int(inflight_by_node.get(node_id, 0) or 0),
                                ),
                            )
                            for node_id in completion_order
                        }
                        _fill_from_quota(refill_quota, node_order=completion_order)
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
        max_in_flight: int = 32,
        receive_batch: int = 1,
        submit_timeout_sec: float = 60.0,
        result_timeout_sec: float = 30.0,
        wait_ms: int = 500,
        raise_on_error: bool = True,
        node_window_factor: float = 2.0,
    ) -> Iterator[Tuple[str, Any]]:
        if self._backend is not None:
            yield from self._backend.unordered(
                payloads,
                task_method=task_method,
                max_in_flight=max_in_flight,
                receive_batch=receive_batch,
                submit_timeout_sec=submit_timeout_sec,
                result_timeout_sec=result_timeout_sec,
                wait_ms=wait_ms,
                raise_on_error=raise_on_error,
                node_window_factor=node_window_factor,
            )
            return
        yield from self.imap_unordered(
            payloads,
            task_method=task_method,
            max_in_flight=max_in_flight,
            receive_batch=receive_batch,
            submit_timeout_sec=submit_timeout_sec,
            result_timeout_sec=result_timeout_sec,
            wait_ms=wait_ms,
            raise_on_error=raise_on_error,
            node_window_factor=node_window_factor,
        )

    def consume_unordered(
        self,
        payloads: Iterable[Dict[str, object]],
        *,
        handle: Callable[[str, Any], Any],
        task_method: str = "",
        max_in_flight: int = 32,
        receive_batch: int = 1,
        submit_timeout_sec: float = 60.0,
        result_timeout_sec: float = 30.0,
        wait_ms: int = 500,
        raise_on_error: bool = True,
        node_window_factor: float = 2.0,
    ) -> int:
        if self._backend is not None:
            return int(
                self._backend.consume_unordered(
                    payloads,
                    handle=handle,
                    task_method=task_method,
                    max_in_flight=max_in_flight,
                    receive_batch=receive_batch,
                    submit_timeout_sec=submit_timeout_sec,
                    result_timeout_sec=result_timeout_sec,
                    wait_ms=wait_ms,
                    raise_on_error=raise_on_error,
                    node_window_factor=node_window_factor,
                )
            )
        if not callable(handle):
            raise TypeError("handle must be callable")
        processed = 0
        for task_id, result in self.unordered(
            payloads,
            task_method=task_method,
            max_in_flight=max_in_flight,
            receive_batch=receive_batch,
            submit_timeout_sec=submit_timeout_sec,
            result_timeout_sec=result_timeout_sec,
            wait_ms=wait_ms,
            raise_on_error=raise_on_error,
            node_window_factor=node_window_factor,
        ):
            handle(task_id, result)
            processed += 1
        return processed

    def map(
        self,
        values: Sequence[Any],
        *,
        arg_name: str = "value",
        task_method: str = "",
        timeout_sec: float = 30.0,
        **shared_kwargs,
    ) -> Sequence[Any]:
        if self._backend is not None:
            return self._backend.map(
                values,
                arg_name=arg_name,
                task_method=task_method,
                timeout_sec=timeout_sec,
                **shared_kwargs,
            )
        resp = self.submit_values(values, arg_name=arg_name, task_method=task_method, **shared_kwargs)
        return self.wait_for_data(expected_count=len(resp.accepted), timeout_sec=timeout_sec)

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

    def is_alive(self) -> bool:
        if self._backend is not None and hasattr(self._backend, "is_alive"):
            return bool(self._backend.is_alive())
        if self._closed:
            return False
        if self.failed:
            return False
        return any(node_id in self._active_nodes for node_id in self._pools)

    def close(self) -> None:
        if self._backend is not None:
            if self._closed:
                return
            self._closed = True
            self._stop_keepalive()
            self._backend.close()
            return
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
        if isinstance(self._backend, NativeTaskBackend) or self._backend is None:
            return f"<TaskPoolSession methods={self.methods} nodes={len(self.node_ids)}>"
        if self._backend is not None:
            return repr(self._backend)
        return f"<TaskPoolSession methods={self.methods} nodes={len(self.node_ids)}>"


class DedicatedTaskServiceSession(TaskPoolSession):
    """Compatibility wrapper that exposes service-backed task execution as a task session."""

    def __init__(
        self,
        *,
        group: "ServiceGroup",
        task_method: str,
        job_id: str = "",
        max_submit_workers: int = 0,
        backend: Optional[_ServiceCompatTaskBackend] = None,
    ) -> None:
        effective_backend = backend or _ServiceCompatTaskBackend(
            group=group,
            task_method=task_method,
            job_id=job_id,
            max_submit_workers=max_submit_workers,
        )
        super().__init__(
            pools={},
            nodes=dict(getattr(group, "nodes", {}) or {}),
            task_method=task_method,
            job_id=effective_backend.job_id,
            backend=effective_backend,
        )

    @classmethod
    def from_infocenter(
        cls,
        *,
        infocenter_target: str,
        job_id: str = "",
        owner_client_id: Optional[str] = None,
        service_name: Optional[str] = None,
        artifact: Optional[Any] = None,
        deps: Optional[Any] = None,
        func: Optional[Callable] = None,
        artifact_path: Union[str, os.PathLike[str], Sequence[Union[str, os.PathLike[str]]]] = "",
        blob: Optional[bytes] = None,
        runtime: str = "py3",
        entry_module: Any = "",
        entry_callable: Any = "run",
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
        backend = _ServiceCompatTaskBackend.from_infocenter(
            infocenter_target=infocenter_target,
            job_id=job_id,
            owner_client_id=owner_client_id,
            service_name=service_name,
            artifact=artifact,
            deps=deps,
            func=func,
            artifact_path=artifact_path,
            blob=blob,
            runtime=runtime,
            entry_module=entry_module,
            entry_callable=entry_callable,
            package_format=package_format,
            dependency_allowlist=dependency_allowlist,
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
            allow_partial=allow_partial,
            min_success_nodes=min_success_nodes,
            timeout_sec=timeout_sec,
            session_cache_dir=session_cache_dir,
        )
        return cls(group=backend._group, task_method=backend.methods[0], job_id=backend.job_id, backend=backend)

    def __repr__(self) -> str:
        return f"<DedicatedTaskServiceSession methods={self.methods} nodes={len(self.node_ids)}>"


def _task_pool_session_from_infocenter(
    cls,
    *,
    infocenter_target: str,
    job_id: str = "",
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
    source_func = entry_func if entry_func is not None else func
    normalized_artifact = _normalize_artifact_input(
        consumer_kind="task",
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
    effective_pool_name = str(pool_name or f"task-pool-{uuid.uuid4().hex[:10]}").strip()

    pools: Dict[str, NativeTaskPoolClient] = {}
    nodes: Dict[str, InfoCenterNode] = {}
    for node in desired_nodes:
        client = NodeControlClient(node.control_addr, timeout_sec=timeout_sec)
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
    session = cls(pools=pools, nodes=nodes, task_method=entry_callable, job_id=job_id)
    session._start_keepalive()
    return session


TaskPoolSession.from_infocenter = classmethod(_task_pool_session_from_infocenter)


class _NativePoolResultAdapter:
    def fetch_result_data(self, task_result: pb2.TaskResult, *, target_path: str = ""):
        del target_path
        if task_result.result:
            return struct_to_dict(task_result.result)
        raise RuntimeError(task_result.error.message or "task failed")



__all__ = [
    "TaskPoolSession",
    "DedicatedTaskServiceSession",
    "_task_pool_session_from_infocenter",
    "_NativePoolResultAdapter",
]
