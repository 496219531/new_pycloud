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
from pycloud_parallel.execution.base import ExecutionItem
from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2


def _now_timestamp() -> timestamp_pb2.Timestamp:
    ts = timestamp_pb2.Timestamp()
    ts.GetCurrentTime()
    return ts


def _resolve_task_results_data(batch: Any, results: Sequence[pb2.TaskResult]) -> List[Any]:
    return [batch.fetch_result_data(item) for item in results]


TaskPoolItem = ExecutionItem


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
    """Native task backend that delegates into the built-in pool session implementation."""

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

    def imap_unordered(self, payloads: Iterable[Dict[str, object]], **kwargs) -> Iterator[Tuple[int, Any]]:
        with self._native():
            yield from self._session.imap_unordered(payloads, **kwargs)

    def unordered(self, payloads: Iterable[Dict[str, object]], **kwargs) -> Iterator[Tuple[int, Any]]:
        with self._native():
            yield from self._session.unordered(payloads, **kwargs)

    async def aunordered(self, payloads: Iterable[Dict[str, object]], **kwargs):
        with self._native():
            async for item in self._session.aunordered(payloads, **kwargs):
                yield item

    def consume_unordered(self, payloads: Iterable[Dict[str, object]], **kwargs) -> int:
        with self._native():
            return int(self._session.consume_unordered(payloads, **kwargs))

    def map(self, values: Sequence[Any], **kwargs) -> Sequence[Any]:
        with self._native():
            return self._session.map(values, **kwargs)

    async def amap(self, values: Sequence[Any], **kwargs) -> Sequence[Any]:
        with self._native():
            return await self._session.amap(values, **kwargs)

    def iter_items(self, *args, **kwargs):
        with self._native():
            yield from self._session.iter_items(*args, **kwargs)

    async def aiter_items(self, *args, **kwargs):
        with self._native():
            async for item in self._session.aiter_items(*args, **kwargs):
                yield item

    def collect_items(self, *args, **kwargs):
        with self._native():
            return self._session.collect_items(*args, **kwargs)

    async def acollect_items(self, *args, **kwargs):
        with self._native():
            return await self._session.acollect_items(*args, **kwargs)

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
