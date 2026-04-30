from __future__ import annotations

"""Executor backend boundary used by NodeControl."""

from typing import Any, Dict, Optional, Protocol

from pycloud_parallel.controlplane.executor_host import ExecutorHostClient

EXECUTOR_BACKEND_SUBPROCESS_HOST = "subprocess_host"
VALID_EXECUTOR_BACKENDS = {EXECUTOR_BACKEND_SUBPROCESS_HOST}


def normalize_executor_backend(value: str) -> str:
    text = str(value or "").strip().lower().replace("-", "_")
    if not text:
        raise ValueError("executor_backend is required")
    if text not in VALID_EXECUTOR_BACKENDS:
        raise ValueError(
            f"executor_backend must be subprocess_host (got {value!r})"
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

    def prepare_artifact(
        self,
        *,
        artifact_spec: Dict[str, Any],
        timeout_sec: float = 30.0,
        scope: str = "",
        key: str = "",
    ) -> Dict[str, Any]: ...

    def call_service(self, *, service_id: str, timeout_sec: float, execute_spec: Dict[str, Any]) -> Dict[str, Any]: ...

    def call_service_stream(self, *, service_id: str, timeout_sec: float, execute_spec: Dict[str, Any]): ...

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
        self._task_worker_capacity = max(1, int(task_worker_capacity or 1))
        self._runtime_client: Optional[ExecutorHostClient] = None
        self._prepare_client: Optional[ExecutorHostClient] = None
        self._service_clients: Dict[str, ExecutorHostClient] = {}
        self._pool_clients: Dict[str, ExecutorHostClient] = {}
        self._closed = False

    def _new_client(self) -> ExecutorHostClient:
        if self._closed:
            raise RuntimeError("executor backend is closed")
        return ExecutorHostClient(task_worker_capacity=self._task_worker_capacity)

    def _ensure_runtime_client(self) -> ExecutorHostClient:
        if self._runtime_client is None or not self._runtime_client.is_alive():
            self._runtime_client = self._new_client()
        return self._runtime_client

    def _ensure_prepare_client(self) -> ExecutorHostClient:
        if self._prepare_client is None or not self._prepare_client.is_alive():
            self._prepare_client = self._new_client()
        return self._prepare_client

    def _ensure_service_client(self, service_id: str) -> ExecutorHostClient:
        key = str(service_id or "").strip()
        client = self._service_clients.get(key)
        if client is None or not client.is_alive():
            client = self._new_client()
            self._service_clients[key] = client
        return client

    def _ensure_pool_client(self, pool_id: str) -> ExecutorHostClient:
        key = str(pool_id or "").strip()
        client = self._pool_clients.get(key)
        if client is None or not client.is_alive():
            client = self._new_client()
            self._pool_clients[key] = client
        return client

    def _service_client(self, service_id: str) -> ExecutorHostClient:
        key = str(service_id or "").strip()
        client = self._service_clients.get(key)
        if client is None:
            raise RuntimeError("service executor host missing")
        if not client.is_alive():
            raise RuntimeError("service executor host died")
        return client

    def _pool_client(self, pool_id: str) -> ExecutorHostClient:
        key = str(pool_id or "").strip()
        client = self._pool_clients.get(key)
        if client is None:
            raise RuntimeError("task pool executor host missing")
        if not client.is_alive():
            raise RuntimeError("task pool executor host died")
        return client

    def is_alive(self) -> bool:
        if self._closed:
            return False
        if self._runtime_client is not None and not self._runtime_client.is_alive():
            return False
        if self._prepare_client is not None and not self._prepare_client.is_alive():
            return False
        return all(client.is_alive() for client in self._service_clients.values()) and all(
            client.is_alive() for client in self._pool_clients.values()
        )

    def close(self, *, shutdown_timeout_sec: float = 2.0) -> None:
        self._closed = True
        clients = []
        if self._runtime_client is not None:
            clients.append(self._runtime_client)
        if self._prepare_client is not None:
            clients.append(self._prepare_client)
        clients.extend(self._service_clients.values())
        clients.extend(self._pool_clients.values())
        self._runtime_client = None
        self._prepare_client = None
        self._service_clients.clear()
        self._pool_clients.clear()
        for client in clients:
            client.close(shutdown_timeout_sec=shutdown_timeout_sec)

    def drain_events(self) -> list[Dict[str, Any]]:
        items: list[Dict[str, Any]] = []
        clients = []
        if self._runtime_client is not None:
            clients.append(self._runtime_client)
        if self._prepare_client is not None:
            clients.append(self._prepare_client)
        clients.extend(self._service_clients.values())
        clients.extend(self._pool_clients.values())
        for client in clients:
            items.extend(client.drain_events())
        return items

    def poll_events(self) -> list[Dict[str, Any]]:
        return self.drain_events()

    def create_service(self, *, service_id: str, worker_count: int) -> None:
        self._ensure_service_client(service_id).create_service(service_id=service_id, worker_count=worker_count)

    def stop_service(self, *, service_id: str) -> None:
        key = str(service_id or "").strip()
        client = self._service_clients.pop(key, None)
        if client is None:
            return
        try:
            client.stop_service(service_id=service_id)
        finally:
            client.close()

    def create_task_pool(self, *, pool_id: str, worker_count: int) -> None:
        self._ensure_pool_client(pool_id).create_task_pool(pool_id=pool_id, worker_count=worker_count)

    def stop_task_pool(self, *, pool_id: str) -> None:
        key = str(pool_id or "").strip()
        client = self._pool_clients.pop(key, None)
        if client is None:
            return
        try:
            client.stop_task_pool(pool_id=pool_id)
        finally:
            client.close()

    def prepare_artifact(
        self,
        *,
        artifact_spec: Dict[str, Any],
        timeout_sec: float = 30.0,
        scope: str = "",
        key: str = "",
    ) -> Dict[str, Any]:
        normalized_scope = str(scope or "").strip().lower()
        normalized_key = str(key or "").strip()
        if normalized_scope == "service" and normalized_key:
            client = self._ensure_service_client(normalized_key)
        elif normalized_scope == "pool" and normalized_key:
            client = self._ensure_pool_client(normalized_key)
        else:
            client = self._ensure_prepare_client()
        return client.prepare_artifact(artifact_spec=artifact_spec, timeout_sec=timeout_sec)

    def call_service(self, *, service_id: str, timeout_sec: float, execute_spec: Dict[str, Any]) -> Dict[str, Any]:
        return self._service_client(service_id).call_service(service_id=service_id, timeout_sec=timeout_sec, execute_spec=execute_spec)

    def call_service_stream(self, *, service_id: str, timeout_sec: float, execute_spec: Dict[str, Any]):
        return self._service_client(service_id).call_service_stream(
            service_id=service_id,
            timeout_sec=timeout_sec,
            execute_spec=execute_spec,
        )

    def warmup_service(self, *, service_id: str, fanout: int, execute_spec: Dict[str, Any]) -> int:
        return self._service_client(service_id).warmup_service(service_id=service_id, fanout=fanout, execute_spec=execute_spec)

    def preload_service(self, *, service_id: str, fanout: int, execute_spec: Dict[str, Any]) -> int:
        return self._service_client(service_id).preload_service(service_id=service_id, fanout=fanout, execute_spec=execute_spec)

    def submit_runtime_task(self, *, runtime_key: str, task_id: str, attempt: int, execute_spec: Dict[str, Any]) -> None:
        self._ensure_runtime_client().submit_runtime_task(runtime_key=runtime_key, task_id=task_id, attempt=attempt, execute_spec=execute_spec)

    def warmup_runtime(self, *, runtime_key: str, fanout: int, execute_spec: Dict[str, Any]) -> int:
        return self._ensure_runtime_client().warmup_runtime(runtime_key=runtime_key, fanout=fanout, execute_spec=execute_spec)

    def submit_pool_task(self, *, pool_id: str, task_id: str, attempt: int, execute_spec: Dict[str, Any]) -> None:
        self._pool_client(pool_id).submit_pool_task(pool_id=pool_id, task_id=task_id, attempt=attempt, execute_spec=execute_spec)

    def warmup_pool(self, *, pool_id: str, fanout: int, execute_spec: Dict[str, Any]) -> int:
        return self._pool_client(pool_id).warmup_pool(pool_id=pool_id, fanout=fanout, execute_spec=execute_spec)

    def preload_pool(self, *, pool_id: str, fanout: int, execute_spec: Dict[str, Any]) -> int:
        return self._pool_client(pool_id).preload_pool(pool_id=pool_id, fanout=fanout, execute_spec=execute_spec)


def create_executor_backend(*, executor_backend: str = "", task_worker_capacity: int = 1) -> ExecutorBackend:
    configured_backend = str(executor_backend or "").strip()
    if not configured_backend:
        from pycloud_parallel.controlplane.config import EXECUTOR_BACKEND

        configured_backend = EXECUTOR_BACKEND
    normalize_executor_backend(configured_backend)
    return SubprocessExecutorBackend(task_worker_capacity=task_worker_capacity)
