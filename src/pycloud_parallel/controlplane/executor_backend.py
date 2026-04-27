from __future__ import annotations

"""Executor backend boundary used by NodeControl."""

from collections import deque
import threading
import time
from typing import Any, Deque, Dict, Optional, Protocol

from pycloud_parallel.controlplane.executor_core import ExecutorCore
from pycloud_parallel.controlplane.executor_host import ExecutorHostClient

EXECUTOR_BACKEND_SUBPROCESS_HOST = "subprocess_host"
EXECUTOR_BACKEND_EMBEDDED = "embedded"
VALID_EXECUTOR_BACKENDS = {EXECUTOR_BACKEND_SUBPROCESS_HOST, EXECUTOR_BACKEND_EMBEDDED}


def normalize_executor_backend(value: str) -> str:
    text = str(value or "").strip().lower().replace("-", "_")
    if not text:
        return EXECUTOR_BACKEND_EMBEDDED
    aliases = {
        "subprocess": EXECUTOR_BACKEND_SUBPROCESS_HOST,
        "host": EXECUTOR_BACKEND_SUBPROCESS_HOST,
        "executor_host": EXECUTOR_BACKEND_SUBPROCESS_HOST,
        "subprocesshost": EXECUTOR_BACKEND_SUBPROCESS_HOST,
        "inprocess": EXECUTOR_BACKEND_EMBEDDED,
        "in_process": EXECUTOR_BACKEND_EMBEDDED,
        "local": EXECUTOR_BACKEND_EMBEDDED,
    }
    text = aliases.get(text, text)
    if text not in VALID_EXECUTOR_BACKENDS:
        raise ValueError(
            "executor_backend must be one of: subprocess_host, embedded "
            f"(got {value!r})"
        )
    return text


class ExecutorBackend(Protocol):
    backend_name: str

    def is_alive(self) -> bool: ...

    def close(self, *, shutdown_timeout_sec: float = 2.0) -> None: ...

    def drain_events(self) -> list[Dict[str, Any]]: ...

    def poll_events(self) -> list[Dict[str, Any]]: ...

    def create_service(self, *, service_id: str, worker_count: int) -> None: ...

    def stop_service(self, *, service_id: str) -> None: ...

    def create_task_pool(self, *, pool_id: str, worker_count: int) -> None: ...

    def stop_task_pool(self, *, pool_id: str) -> None: ...

    def call_service(self, *, service_id: str, timeout_sec: float, execute_spec: Dict[str, Any]) -> Dict[str, Any]: ...

    def warmup_service(self, *, service_id: str, fanout: int, execute_spec: Dict[str, Any]) -> int: ...

    def preload_service(self, *, service_id: str, fanout: int, execute_spec: Dict[str, Any]) -> int: ...

    def submit_runtime_task(self, *, runtime_key: str, task_id: str, attempt: int, execute_spec: Dict[str, Any]) -> None: ...

    def warmup_runtime(self, *, runtime_key: str, fanout: int, execute_spec: Dict[str, Any]) -> int: ...

    def submit_pool_task(self, *, pool_id: str, task_id: str, attempt: int, execute_spec: Dict[str, Any]) -> None: ...

    def warmup_pool(self, *, pool_id: str, fanout: int, execute_spec: Dict[str, Any]) -> int: ...

    def preload_pool(self, *, pool_id: str, fanout: int, execute_spec: Dict[str, Any]) -> int: ...


class SubprocessExecutorBackend:
    backend_name = EXECUTOR_BACKEND_SUBPROCESS_HOST

    def __init__(self, *, task_worker_capacity: int = 1) -> None:
        self._client = ExecutorHostClient(task_worker_capacity=task_worker_capacity)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._client, name)

    def is_alive(self) -> bool:
        return self._client.is_alive()

    def close(self, *, shutdown_timeout_sec: float = 2.0) -> None:
        self._client.close(shutdown_timeout_sec=shutdown_timeout_sec)

    def drain_events(self) -> list[Dict[str, Any]]:
        return self._client.drain_events()

    def poll_events(self) -> list[Dict[str, Any]]:
        return self.drain_events()

    def create_service(self, *, service_id: str, worker_count: int) -> None:
        self._client.create_service(service_id=service_id, worker_count=worker_count)

    def stop_service(self, *, service_id: str) -> None:
        self._client.stop_service(service_id=service_id)

    def create_task_pool(self, *, pool_id: str, worker_count: int) -> None:
        self._client.create_task_pool(pool_id=pool_id, worker_count=worker_count)

    def stop_task_pool(self, *, pool_id: str) -> None:
        self._client.stop_task_pool(pool_id=pool_id)

    def call_service(self, *, service_id: str, timeout_sec: float, execute_spec: Dict[str, Any]) -> Dict[str, Any]:
        return self._client.call_service(service_id=service_id, timeout_sec=timeout_sec, execute_spec=execute_spec)

    def warmup_service(self, *, service_id: str, fanout: int, execute_spec: Dict[str, Any]) -> int:
        return self._client.warmup_service(service_id=service_id, fanout=fanout, execute_spec=execute_spec)

    def preload_service(self, *, service_id: str, fanout: int, execute_spec: Dict[str, Any]) -> int:
        return self._client.preload_service(service_id=service_id, fanout=fanout, execute_spec=execute_spec)

    def submit_runtime_task(self, *, runtime_key: str, task_id: str, attempt: int, execute_spec: Dict[str, Any]) -> None:
        self._client.submit_runtime_task(runtime_key=runtime_key, task_id=task_id, attempt=attempt, execute_spec=execute_spec)

    def warmup_runtime(self, *, runtime_key: str, fanout: int, execute_spec: Dict[str, Any]) -> int:
        return self._client.warmup_runtime(runtime_key=runtime_key, fanout=fanout, execute_spec=execute_spec)

    def submit_pool_task(self, *, pool_id: str, task_id: str, attempt: int, execute_spec: Dict[str, Any]) -> None:
        self._client.submit_pool_task(pool_id=pool_id, task_id=task_id, attempt=attempt, execute_spec=execute_spec)

    def warmup_pool(self, *, pool_id: str, fanout: int, execute_spec: Dict[str, Any]) -> int:
        return self._client.warmup_pool(pool_id=pool_id, fanout=fanout, execute_spec=execute_spec)

    def preload_pool(self, *, pool_id: str, fanout: int, execute_spec: Dict[str, Any]) -> int:
        return self._client.preload_pool(pool_id=pool_id, fanout=fanout, execute_spec=execute_spec)


class EmbeddedExecutorBackend:
    """In-node executor manager; removes the executor_host hop but shares the same worker core."""

    backend_name = EXECUTOR_BACKEND_EMBEDDED

    def __init__(self, *, task_worker_capacity: int = 1) -> None:
        self._responses: Dict[str, Dict[str, Any]] = {}
        self._expired_requests: set[str] = set()
        self._async_events: Deque[Dict[str, Any]] = deque()
        self._cv = threading.Condition()
        self._core_lock = threading.RLock()
        self._seq = 0
        self._closed = False
        self._core = ExecutorCore(
            task_worker_capacity=task_worker_capacity,
            emit_response=self._emit_item,
            emit_event=self._emit_item,
        )

    def is_alive(self) -> bool:
        return not self._closed

    def close(self, *, shutdown_timeout_sec: float = 2.0) -> None:
        del shutdown_timeout_sec
        if self._closed:
            return
        self._closed = True
        with self._core_lock:
            self._core.close()
        with self._cv:
            self._cv.notify_all()

    def _emit_item(self, item: Dict[str, Any]) -> None:
        if not isinstance(item, dict):
            return
        kind = str(item.get("kind", "") or "")
        with self._cv:
            if kind == "response":
                request_id = str(item.get("request_id", "") or "")
                if request_id in self._expired_requests:
                    self._expired_requests.discard(request_id)
                    return
                self._responses[request_id] = dict(item)
            else:
                self._async_events.append(dict(item))
            self._cv.notify_all()

    def drain_events(self) -> list[Dict[str, Any]]:
        with self._core_lock:
            self._core.poll_once()
        with self._cv:
            items = list(self._async_events)
            self._async_events.clear()
            return items

    def poll_events(self) -> list[Dict[str, Any]]:
        return self.drain_events()

    def _request(self, action: str, *, payload: Optional[Dict[str, Any]] = None, timeout_sec: float = 10.0) -> Dict[str, Any]:
        if self._closed and action != "shutdown":
            raise RuntimeError("executor backend is closed")
        with self._cv:
            self._seq += 1
            request_id = f"embedded-req-{self._seq}"
        with self._core_lock:
            self._core.handle_request(request_id, action, dict(payload or {}))
            self._core.poll_once()
        deadline = time.monotonic() + max(0.1, float(timeout_sec))
        while True:
            with self._cv:
                response = self._responses.pop(request_id, None)
                if response is not None:
                    return response
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    self._expired_requests.add(request_id)
                    raise TimeoutError(f"embedded executor request timed out: {action}")
                wait_for = min(0.05, remaining)
            with self._core_lock:
                self._core.poll_once()
            with self._cv:
                self._cv.wait(timeout=wait_for)

    def _request_action(
        self,
        action: str,
        *,
        payload: Dict[str, Any],
        timeout_sec: float = 30.0,
        raise_on_error: bool = True,
    ) -> Dict[str, Any]:
        resp = self._request(action, payload=payload, timeout_sec=timeout_sec)
        if raise_on_error and not resp.get("ok", False):
            raise RuntimeError(str(resp.get("error", f"{action} failed")))
        return resp

    def create_service(self, *, service_id: str, worker_count: int) -> None:
        self._request_action("create_service", payload={"service_id": service_id, "worker_count": int(worker_count)})

    def stop_service(self, *, service_id: str) -> None:
        self._request_action("stop_service", payload={"service_id": service_id})

    def create_task_pool(self, *, pool_id: str, worker_count: int) -> None:
        self._request_action("create_task_pool", payload={"pool_id": pool_id, "worker_count": int(worker_count)})

    def stop_task_pool(self, *, pool_id: str) -> None:
        self._request_action("stop_task_pool", payload={"pool_id": pool_id})

    def call_service(self, *, service_id: str, timeout_sec: float, execute_spec: Dict[str, Any]) -> Dict[str, Any]:
        return self._request_action(
            "call_service",
            payload={"service_id": service_id, "timeout_sec": float(timeout_sec), **dict(execute_spec)},
            timeout_sec=max(1.0, float(timeout_sec) + 2.0),
            raise_on_error=False,
        )

    def _warmup(self, action: str, *, identity: Dict[str, Any], fanout: int, execute_spec: Dict[str, Any]) -> int:
        resp = self._request_action(
            action,
            payload={**dict(identity), "fanout": int(fanout), **dict(execute_spec)},
            timeout_sec=max(1.0, float(fanout) + 5.0),
        )
        return int(resp.get("submitted", 0) or 0)

    def _preload(self, action: str, *, identity: Dict[str, Any], fanout: int, execute_spec: Dict[str, Any]) -> int:
        return self._warmup(action, identity=identity, fanout=fanout, execute_spec=execute_spec)

    def warmup_service(self, *, service_id: str, fanout: int, execute_spec: Dict[str, Any]) -> int:
        return self._warmup("warmup_service", identity={"service_id": service_id}, fanout=fanout, execute_spec=execute_spec)

    def preload_service(self, *, service_id: str, fanout: int, execute_spec: Dict[str, Any]) -> int:
        return self._preload("preload_service", identity={"service_id": service_id}, fanout=fanout, execute_spec=execute_spec)

    def submit_runtime_task(self, *, runtime_key: str, task_id: str, attempt: int, execute_spec: Dict[str, Any]) -> None:
        self._request_action(
            "submit_runtime_task",
            payload={"runtime_key": runtime_key, "task_id": task_id, "attempt": int(attempt), **dict(execute_spec)},
        )

    def warmup_runtime(self, *, runtime_key: str, fanout: int, execute_spec: Dict[str, Any]) -> int:
        return self._warmup("warmup_runtime", identity={"runtime_key": runtime_key}, fanout=fanout, execute_spec=execute_spec)

    def submit_pool_task(self, *, pool_id: str, task_id: str, attempt: int, execute_spec: Dict[str, Any]) -> None:
        self._request_action(
            "submit_pool_task",
            payload={"pool_id": pool_id, "task_id": task_id, "attempt": int(attempt), **dict(execute_spec)},
        )

    def warmup_pool(self, *, pool_id: str, fanout: int, execute_spec: Dict[str, Any]) -> int:
        return self._warmup("warmup_pool", identity={"pool_id": pool_id}, fanout=fanout, execute_spec=execute_spec)

    def preload_pool(self, *, pool_id: str, fanout: int, execute_spec: Dict[str, Any]) -> int:
        return self._preload("preload_pool", identity={"pool_id": pool_id}, fanout=fanout, execute_spec=execute_spec)


def create_executor_backend(*, executor_backend: str = "", task_worker_capacity: int = 1) -> ExecutorBackend:
    backend = normalize_executor_backend(executor_backend)
    if backend == EXECUTOR_BACKEND_EMBEDDED:
        return EmbeddedExecutorBackend(task_worker_capacity=task_worker_capacity)
    return SubprocessExecutorBackend(task_worker_capacity=task_worker_capacity)
