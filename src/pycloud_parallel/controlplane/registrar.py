from __future__ import annotations

"""Background registrar for NodeControl -> InfoCenter heartbeats."""

import json
import threading
import uuid
from datetime import datetime, timedelta, timezone
from typing import Callable, Dict, Iterable, Optional
from importlib import metadata as importlib_metadata

from pycloud_parallel.controlplane.infocenter_client import InfoCenterClient
from pycloud_parallel.controlplane.node_capability import detect_local_node_capability
from pycloud_parallel.controlplane.node.state import NodeControlState
from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2


def _pycloud_version() -> str:
    try:
        import pycloud_parallel

        runtime_version = str(getattr(pycloud_parallel, "__version__", "") or "").strip()
        if runtime_version:
            return runtime_version
    except Exception:
        pass
    try:
        return str(importlib_metadata.version("pycloud-parallel"))
    except Exception:
        return "unknown"


class NodeInfoCenterRegistrar:
    def __init__(
        self,
        *,
        infocenter_addr: str,
        node_id: str,
        control_addr: str,
        state: NodeControlState,
        capacity: int,
        queue_capacity: int,
        tags: Optional[Iterable[str]] = None,
        version: str = "",
        metadata: Optional[Dict[str, str]] = None,
        fallback_heartbeat_sec: int = 10,
        rpc_timeout_sec: float = 5.0,
    ) -> None:
        self.infocenter_addr = infocenter_addr
        self.node_id = node_id
        self.node_instance_id = f"{str(node_id or 'node').strip() or 'node'}-{uuid.uuid4().hex[:12]}"
        self.control_addr = control_addr
        self.state = state
        self.capacity = max(1, int(capacity))
        self.queue_capacity = max(1, int(queue_capacity))
        self.tags = list(tags or [])
        self.version = version
        self.metadata = dict(metadata or {})
        self.fallback_heartbeat_sec = max(1, int(fallback_heartbeat_sec))
        self.rpc_timeout_sec = max(0.5, float(rpc_timeout_sec))

        self._client = InfoCenterClient(self.infocenter_addr, timeout_sec=self.rpc_timeout_sec)
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._registered = False
        self._next_hb_sec = self.fallback_heartbeat_sec
        self._sync_lock = threading.Lock()

    @staticmethod
    def _pycloud_version() -> str:
        return _pycloud_version()

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._loop, name=f"node-registrar-{self.node_id}", daemon=True)
        self._thread.start()

    def close(self) -> None:
        self._stop_event.set()
        thread = self._thread
        if thread is not None:
            thread.join(timeout=2.0)
            if thread.is_alive():
                return
            self._thread = None
        with self._sync_lock:
            self._client.close()

    def sync_now(self) -> bool:
        if not self._sync_lock.acquire(blocking=False):
            return False
        try:
            should_register = not self._registered
        finally:
            self._sync_lock.release()
        try:
            return self._register_once() if should_register else self._heartbeat_once()
        except Exception:
            with self._sync_lock:
                self._registered = False
            return False

    def _register_once(self) -> bool:
        metadata = dict(self.metadata)
        metadata.update(self.state.service_timing_metadata())
        metadata["pycloud_version"] = self._pycloud_version()
        task_pools = [
            {
                "pool_id": item.pool_id,
                "owner_client_id": item.owner_client_id,
                "pool_name": item.pool_name,
                "code_version": item.code_version,
                "status": item.status,
                "worker_count": int(item.worker_count),
                "task_count": int(item.task_count),
                "inflight": int(item.inflight),
                "created_at": item.created_at.isoformat(),
                "last_heartbeat_at": item.last_heartbeat_at.isoformat(),
                "lease_expire_at": item.lease_expire_at.isoformat(),
            }
            for item in self.state.task_pool_reports().values()
        ]
        resp = self._client.register_node(
            node_id=self.node_id,
            node_instance_id=self.node_instance_id,
            control_addr=self.control_addr,
            capacity=self.capacity,
            queue_capacity=self.queue_capacity,
            tags=self.tags,
            version=self.version,
            metadata=metadata,
            services=self.state.service_report_payloads(),
            task_pools=task_pools,
            active_runtimes=self.state.active_runtime_keys(limit=10),
            service_worker_capacity=self.state.service_worker_capacity,
            service_worker_used=self.state.service_worker_used(),
            task_pool_worker_capacity=self.state.task_pool_worker_capacity,
            task_pool_worker_used=self.state.task_pool_worker_used(),
            python_version=self.state.python_version,
            capability=self.state.node_capability(),
        )
        with self._sync_lock:
            self._registered = True
            self._next_hb_sec = max(1, int(resp.get("heartbeat_interval_sec", self.fallback_heartbeat_sec) or self.fallback_heartbeat_sec))
        return True

    def _heartbeat_once(self) -> bool:
        metrics = self.state.metrics()
        metadata = dict(self.metadata)
        metadata.update(self.state.service_timing_metadata())
        metadata["pycloud_version"] = self._pycloud_version()
        task_pools = [
            {
                "pool_id": item.pool_id,
                "owner_client_id": item.owner_client_id,
                "pool_name": item.pool_name,
                "code_version": item.code_version,
                "status": item.status,
                "worker_count": int(item.worker_count),
                "task_count": int(item.task_count),
                "inflight": int(item.inflight),
                "created_at": item.created_at.isoformat(),
                "last_heartbeat_at": item.last_heartbeat_at.isoformat(),
                "lease_expire_at": item.lease_expire_at.isoformat(),
            }
            for item in self.state.task_pool_reports().values()
        ]
        resp = self._client.heartbeat_node(
            node_id=self.node_id,
            node_instance_id=self.node_instance_id,
            healthy=True,
            metrics={
                "queued": metrics["queued"],
                "inflight": metrics["inflight"],
                "running": metrics["running"],
                "credit": metrics["credit"],
                "cpu_percent": 0.0,
                "mem_percent": 0.0,
            },
            metadata=metadata,
            services=self.state.service_report_payloads(),
            task_pools=task_pools,
            active_runtimes=self.state.active_runtime_keys(limit=10),
            service_worker_capacity=self.state.service_worker_capacity,
            service_worker_used=self.state.service_worker_used(),
            task_pool_worker_capacity=self.state.task_pool_worker_capacity,
            task_pool_worker_used=self.state.task_pool_worker_used(),
            python_version=self.state.python_version,
            capability=self.state.node_capability(),
        )
        with self._sync_lock:
            if not resp.get("accepted", False):
                self._registered = False
                return False
            self._next_hb_sec = max(1, int(resp.get("next_heartbeat_in_sec", self.fallback_heartbeat_sec) or self.fallback_heartbeat_sec))
        return True

    def _loop(self) -> None:
        while not self._stop_event.is_set():
            self.sync_now()
            with self._sync_lock:
                wait_sec = self._next_hb_sec if self._registered else self.fallback_heartbeat_sec
            self._stop_event.wait(max(1, int(wait_sec)))


class JobOrchestratorInfoCenterRegistrar:
    def __init__(
        self,
        *,
        infocenter_addr: str,
        node_id: str,
        service_id: str,
        service_name: str,
        http_base_url_provider: Callable[[], str],
        status_provider: Callable[[], Dict[str, object]],
        queue_capacity: int = 4000,
        tags: Optional[Iterable[str]] = None,
        version: str = "",
        metadata: Optional[Dict[str, str]] = None,
        fallback_heartbeat_sec: int = 10,
        rpc_timeout_sec: float = 5.0,
    ) -> None:
        self.infocenter_addr = str(infocenter_addr or "").strip()
        self.node_id = str(node_id or "job-orchestrator").strip() or "job-orchestrator"
        self.node_instance_id = f"{self.node_id}-{uuid.uuid4().hex[:12]}"
        self.service_id = str(service_id or "").strip()
        self.service_name = str(service_name or "").strip()
        self.http_base_url_provider = http_base_url_provider
        self.status_provider = status_provider
        self.queue_capacity = max(1, int(queue_capacity or 1))
        self.tags = list(tags or ["job"])
        self.version = str(version or "")
        self.metadata = dict(metadata or {})
        self.fallback_heartbeat_sec = max(1, int(fallback_heartbeat_sec or 1))
        self.rpc_timeout_sec = max(0.5, float(rpc_timeout_sec or 0.5))

        self._client = InfoCenterClient(self.infocenter_addr, timeout_sec=self.rpc_timeout_sec)
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._registered = False
        self._next_hb_sec = self.fallback_heartbeat_sec
        self._sync_lock = threading.Lock()

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._loop,
            name=f"job-registrar-{self.node_id}",
            daemon=True,
        )
        self._thread.start()

    def close(self) -> None:
        self._stop_event.set()
        thread = self._thread
        if thread is not None:
            thread.join(timeout=2.0)
            if thread.is_alive():
                return
            self._thread = None
        with self._sync_lock:
            self._client.close()

    def sync_now(self) -> bool:
        if not self._sync_lock.acquire(blocking=False):
            return False
        try:
            should_register = not self._registered
        finally:
            self._sync_lock.release()
        try:
            return self._register_once() if should_register else self._heartbeat_once()
        except Exception:
            with self._sync_lock:
                self._registered = False
            return False

    def _build_metadata(self) -> Dict[str, str]:
        snapshot = dict(self.status_provider() or {})
        metadata = dict(self.metadata)
        metadata["component"] = "job-orchestrator"
        metadata["pycloud_version"] = _pycloud_version()
        metadata["current_job_id"] = str(snapshot.get("current_job_id", "") or "")
        metadata["current_job_status"] = str(snapshot.get("current_job_status", "") or "")
        metadata["job_waiting"] = str(int(snapshot.get("waiting", 0) or 0))
        metadata["job_running"] = str(int(snapshot.get("running", 0) or 0))
        metadata["job_terminal"] = str(int(snapshot.get("terminal", 0) or 0))
        recent_jobs = snapshot.get("recent_jobs") or []
        if isinstance(recent_jobs, list):
            try:
                metadata["job_recent"] = json.dumps(recent_jobs, ensure_ascii=False, separators=(",", ":"))
            except Exception:
                metadata["job_recent"] = "[]"
        waiting_jobs = snapshot.get("waiting_jobs") or []
        if isinstance(waiting_jobs, list):
            try:
                metadata["job_waiting_list"] = json.dumps(waiting_jobs, ensure_ascii=False, separators=(",", ":"))
            except Exception:
                metadata["job_waiting_list"] = "[]"
        return metadata

    def _build_service_payload(self, *, healthy: bool) -> Dict[str, object]:
        snapshot = dict(self.status_provider() or {})
        running = max(0, int(snapshot.get("running", 0) or 0))
        base_url = str(self.http_base_url_provider() or "").strip().rstrip("/")
        http_base_url = f"{base_url}/svc/{self.service_id}" if base_url else ""
        lease_expire_at = datetime.now(timezone.utc) + timedelta(
            seconds=max(5, int(self._next_hb_sec or self.fallback_heartbeat_sec) * 2)
        )
        return {
            "service_name": self.service_name,
            "service_id": self.service_id,
            "status": int(pb2.SERVICE_STATUS_RUNNING if healthy else pb2.SERVICE_STATUS_UNSPECIFIED),
            "worker_count": 1,
            "alive_workers": 1 if healthy else 0,
            "in_flight": running,
            "received_count": max(0, int(snapshot.get("job_count", 0) or 0)),
            "returned_count": max(0, int(snapshot.get("terminal", 0) or 0)),
            "lease_expire_at": lease_expire_at.isoformat(),
            "http_base_url": http_base_url,
        }

    def _metrics_payload(self) -> Dict[str, object]:
        snapshot = dict(self.status_provider() or {})
        waiting = max(0, int(snapshot.get("waiting", 0) or 0))
        running = max(0, int(snapshot.get("running", 0) or 0))
        return {
            "queued": waiting,
            "inflight": running,
            "running": running,
            "credit": max(0, int(self.queue_capacity) - waiting - running),
            "cpu_percent": 0.0,
            "mem_percent": 0.0,
        }

    def _register_once(self) -> bool:
        resp = self._client.register_node(
            node_id=self.node_id,
            node_instance_id=self.node_instance_id,
            control_addr="",
            capacity=1,
            queue_capacity=self.queue_capacity,
            tags=self.tags,
            version=self.version,
            metadata=self._build_metadata(),
            services=[self._build_service_payload(healthy=True)],
            service_worker_capacity=1,
            service_worker_used=1,
            python_version="py3",
            capability=detect_local_node_capability(),
        )
        with self._sync_lock:
            self._registered = True
            self._next_hb_sec = max(
                1,
                int(resp.get("heartbeat_interval_sec", self.fallback_heartbeat_sec) or self.fallback_heartbeat_sec),
            )
        return True

    def _heartbeat_once(self) -> bool:
        resp = self._client.heartbeat_node(
            node_id=self.node_id,
            node_instance_id=self.node_instance_id,
            healthy=True,
            metrics=self._metrics_payload(),
            metadata=self._build_metadata(),
            services=[self._build_service_payload(healthy=True)],
            service_worker_capacity=1,
            service_worker_used=1,
            python_version="py3",
            capability=detect_local_node_capability(),
        )
        with self._sync_lock:
            if not resp.get("accepted", False):
                self._registered = False
                return False
            self._next_hb_sec = max(
                1,
                int(resp.get("next_heartbeat_in_sec", self.fallback_heartbeat_sec) or self.fallback_heartbeat_sec),
            )
        return True

    def _loop(self) -> None:
        while not self._stop_event.is_set():
            self.sync_now()
            with self._sync_lock:
                wait_sec = self._next_hb_sec if self._registered else self.fallback_heartbeat_sec
            self._stop_event.wait(max(1, int(wait_sec)))
