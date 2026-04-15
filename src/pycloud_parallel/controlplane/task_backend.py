from __future__ import annotations

"""Task execution backends and task-session helpers."""

import asyncio
import contextlib
from collections import deque
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
import queue
import threading
import time
from typing import Any, Callable, Dict, Iterable, Iterator, List, Optional, Sequence, Set, Tuple
import uuid

from google.protobuf import timestamp_pb2

from pycloud_parallel.controlplane.config import OBJECT_CHUNK_SIZE_BYTES
from pycloud_parallel.controlplane.session_handle import ExecutionReplicaHandle
from pycloud_parallel.controlplane.serialization import dict_to_struct, struct_to_dict
from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2


def _now_timestamp() -> timestamp_pb2.Timestamp:
    ts = timestamp_pb2.Timestamp()
    ts.GetCurrentTime()
    return ts


def _resolve_task_results_data(batch: Any, results: Sequence[pb2.TaskResult]) -> List[Any]:
    return [batch.fetch_result_data(item) for item in results]


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


class NativeTaskBackend:
    """Native task backend that delegates into TaskPoolSession's built-in implementation."""

    def __init__(self, session: Any) -> None:
        self._session = session
        self._dispatch_lock = threading.Lock()

    @contextlib.contextmanager
    def _native(self):
        with self._dispatch_lock:
            original = self._session._backend
            self._session._backend = None
            try:
                yield
            finally:
                self._session._backend = original

    @property
    def replicas(self) -> Dict[str, ExecutionReplicaHandle]:
        return self._session._pools

    @property
    def client_id(self) -> str:
        first = next(iter(self._session._pools.values()))
        return first.owner_client_id

    @property
    def job_id(self) -> str:
        return self._session._job_id

    @property
    def code_version(self) -> str:
        first = next(iter(self._session._pools.values()))
        return first.code_version

    @property
    def node_ids(self) -> Sequence[str]:
        return [self._session.nodes[key].node_id if key in self._session.nodes else key for key in self._session._pools.keys()]

    @property
    def node_instance_ids(self) -> Sequence[str]:
        return list(self._session._pools.keys())

    @property
    def methods(self) -> List[str]:
        return [self._session._task_method]

    def submit_payloads(self, payloads: Sequence[Dict[str, object]], **kwargs) -> pb2.SubmitTasksResponse:
        with self._native():
            return self._session.submit_payloads(payloads, **kwargs)

    def iter_results(self, **kwargs) -> Iterator[pb2.TaskResult]:
        with self._native():
            yield from self._session.iter_results(**kwargs)

    def wait_for_results(self, **kwargs) -> Sequence[pb2.TaskResult]:
        with self._native():
            return self._session.wait_for_results(**kwargs)

    def wait_for_data(self, **kwargs) -> Sequence[Any]:
        with self._native():
            return self._session.wait_for_data(**kwargs)

    def submit_values(self, values: Sequence[Any], **kwargs) -> pb2.SubmitTasksResponse:
        with self._native():
            return self._session.submit_values(values, **kwargs)

    def update_globals(self, values: Dict[str, object]) -> str:
        with self._native():
            return self._session.update_globals(values)

    def status_map(self) -> Dict[str, pb2.TaskPoolStatusInfo]:
        with self._native():
            return self._session.status_map()

    def is_alive(self) -> bool:
        return (not self._session._closed) and (not self._session.failed) and any(
            node_id in self._session._active_nodes for node_id in self._session._pools
        )

    def imap_unordered(self, payloads: Iterable[Dict[str, object]], **kwargs) -> Iterator[Tuple[str, Any]]:
        with self._native():
            yield from self._session.imap_unordered(payloads, **kwargs)

    def unordered(self, payloads: Iterable[Dict[str, object]], **kwargs) -> Iterator[Tuple[str, Any]]:
        with self._native():
            yield from self._session.unordered(payloads, **kwargs)

    def consume_unordered(self, payloads: Iterable[Dict[str, object]], **kwargs) -> int:
        with self._native():
            return int(self._session.consume_unordered(payloads, **kwargs))

    def map(self, values: Sequence[Any], **kwargs) -> Sequence[Any]:
        with self._native():
            return self._session.map(values, **kwargs)

    def cancel_job(self, **kwargs) -> pb2.CancelJobResponse:
        with self._native():
            return self._session.cancel_job(**kwargs)

    def call_sync(self, method: str, **kwargs) -> Any:
        with self._native():
            return self._session.call_sync(method, **kwargs)

    async def call(self, method: str, **kwargs) -> Any:
        with self._native():
            return await self._session.call(method, **kwargs)

    def __getattr__(self, name: str):
        with self._native():
            return self._session.__getattr__(name)

    def close(self) -> None:
        with self._native():
            self._session.close()

    def __repr__(self) -> str:
        return f"<NativeTaskBackend methods={self.methods} nodes={len(self.node_ids)}>"


class _PoolResultAdapter:
    def __init__(self, session: Any) -> None:
        self._session = session

    def fetch_result_data(self, task_result: pb2.TaskResult, *, target_path: str = ""):
        del target_path
        if task_result.result:
            return struct_to_dict(task_result.result)
        raise RuntimeError(task_result.error.message or "task failed")


class _ServiceCompatTaskBackend:
    """Dedicated temporary task pool backed by a hidden ServiceGroup."""

    def __init__(
        self,
        *,
        group: Any,
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

    @property
    def replicas(self) -> Dict[str, ExecutionReplicaHandle]:
        return self._group.replicas

    def is_alive(self) -> bool:
        return self._group.is_alive()

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
                continue
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

    def update_globals(self, values: Dict[str, object]) -> str:
        if self._closed:
            raise RuntimeError("task pool session is closed")
        return self._group.update_globals(values)

    def status_map(self) -> Dict[str, pb2.ServiceStatusInfo]:
        if self._closed:
            raise RuntimeError("task pool session is closed")
        return self._group.status_map()

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
        del raise_on_error, node_window_factor
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

    def __enter__(self) -> "_ServiceCompatTaskBackend":
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
        return f"<_ServiceCompatTaskBackend methods={self.methods} nodes={len(self.node_ids)}>"

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
        artifact_path: Any = "",
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
    ) -> "_ServiceCompatTaskBackend":
        from pycloud_parallel.controlplane.client import ServiceGroup

        effective_service_name = str(service_name or "").strip() or f"task-pool-{uuid.uuid4().hex[:12]}"
        group = ServiceGroup.deploy_from_infocenter(
            infocenter_target=infocenter_target,
            owner_client_id=owner_client_id,
            service_name=effective_service_name,
            artifact=artifact,
            deps=deps,
            func=func,
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
