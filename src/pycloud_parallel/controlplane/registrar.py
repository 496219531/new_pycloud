from __future__ import annotations

"""Background registrar for NodeControl -> InfoCenter heartbeats."""

import logging
import threading
import time
import uuid
from typing import Dict, Iterable, Optional
from importlib import metadata as importlib_metadata
from urllib.error import URLError

from pycloud_parallel.controlplane.infocenter_client import InfoCenterClient
from pycloud_parallel.controlplane.node.state import NodeControlState

logger = logging.getLogger(__name__)


def _is_expected_connect_failure(exc: BaseException) -> bool:
    if isinstance(exc, (ConnectionError, TimeoutError, OSError)):
        return True
    if isinstance(exc, URLError):
        reason = getattr(exc, "reason", None)
        return reason is None or isinstance(reason, (ConnectionError, TimeoutError, OSError))
    return False


def _error_summary(exc: BaseException) -> str:
    reason = getattr(exc, "reason", None)
    if reason is not None:
        return repr(reason)
    return repr(exc)


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
        self.node_instance_id = str(getattr(state, "node_instance_id", "") or "").strip() or f"{str(node_id or 'node').strip() or 'node'}-{uuid.uuid4().hex[:12]}"
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
        self._wake_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._registered = False
        self._next_hb_sec = self.fallback_heartbeat_sec
        self._lease_ttl_sec = max(self.fallback_heartbeat_sec * 3, self.fallback_heartbeat_sec + 1)
        self._last_successful_sync_at = 0.0
        self._sync_lock = threading.Lock()

    @staticmethod
    def _pycloud_version() -> str:
        return _pycloud_version()

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._wake_event.clear()
        self._thread = threading.Thread(target=self._loop, name=f"node-registrar-{self.node_id}", daemon=True)
        self._thread.start()

    def close(self) -> None:
        self._stop_event.set()
        self._wake_event.set()
        thread = self._thread
        if thread is not None and thread is not threading.current_thread():
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
            was_registered = bool(self._registered)
            should_register = not was_registered
        finally:
            self._sync_lock.release()
        try:
            ok = self._register_once() if should_register else self._heartbeat_once()
            if was_registered and not ok:
                self._close_state_if_registration_lost("registration no longer accepted")
            return ok
        except Exception as exc:
            if _is_expected_connect_failure(exc):
                logger.warning(
                    "[Registrar] node sync deferred node_id=%s node_instance_id=%s should_register=%s error=%s",
                    self.node_id,
                    self.node_instance_id,
                    should_register,
                    _error_summary(exc),
                )
                logger.debug("[Registrar] node sync traceback", exc_info=True)
            else:
                logger.exception(
                    "[Registrar] node sync failed node_id=%s node_instance_id=%s should_register=%s",
                    self.node_id,
                    self.node_instance_id,
                    should_register,
                )
            with self._sync_lock:
                self._registered = False
            self._self_fence_if_lease_expired("infocenter heartbeat lease expired")
            if was_registered or not _is_expected_connect_failure(exc):
                self._close_state_if_registration_lost(_error_summary(exc))
            return False

    def request_sync(self) -> None:
        self._wake_event.set()

    def _close_state_if_registration_lost(self, reason: str) -> None:
        if not bool(getattr(self.state, "close_on_registration_lost", False)):
            return
        with self._sync_lock:
            if bool(self._registered):
                return
        try:
            logger.warning(
                "[Registrar] closing state after registration lost node_id=%s node_instance_id=%s reason=%s",
                self.node_id,
                self.node_instance_id,
                str(reason or "registration lost"),
            )
            self.state.close()
        except Exception:
            logger.exception(
                "[Registrar] close after registration lost failed node_id=%s node_instance_id=%s",
                self.node_id,
                self.node_instance_id,
            )

    def _reset_state_after_fence(self, reason: str) -> None:
        reset = getattr(self.state, "reset_execution_state", None)
        try:
            if callable(reset):
                reset(reason=reason or "node_instance_id fenced")
        finally:
            self._stop_event.set()
            self._wake_event.set()

    def _self_fence_if_lease_expired(self, reason: str) -> bool:
        with self._sync_lock:
            last_success = float(self._last_successful_sync_at or 0.0)
            lease_ttl = max(1.0, float(self._lease_ttl_sec or self.fallback_heartbeat_sec))
            if not last_success or (time.monotonic() - last_success) <= lease_ttl:
                return False
            self._registered = False
        logger.warning(
            "[Registrar] node self fence node_id=%s node_instance_id=%s reason=%s",
            self.node_id,
            self.node_instance_id,
            str(reason or "infocenter heartbeat lease expired"),
        )
        self._reset_state_after_fence(reason or "infocenter heartbeat lease expired")
        return True

    def _register_once(self) -> bool:
        metadata = dict(self.metadata)
        metadata.update(self.state.service_timing_metadata())
        metadata["pycloud_version"] = self._pycloud_version()
        accept_service_deploy = bool(getattr(self.state, "accept_service_deploy", True))
        metadata["accept_service_deploy"] = "true" if accept_service_deploy else "false"
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
                "failure_reason": str(getattr(item, "failure_reason", "") or ""),
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
            services=self.state.service_report_payloads(include_stopped=True),
            task_pools=task_pools,
            active_runtimes=self.state.active_runtime_keys(limit=10),
            service_worker_capacity=self.state.service_worker_capacity,
            service_worker_used=self.state.service_worker_used(),
            task_pool_worker_capacity=self.state.task_pool_worker_capacity,
            task_pool_worker_used=self.state.task_pool_worker_used(),
            accept_service_deploy=accept_service_deploy,
            python_version=self.state.python_version,
            capability=self.state.node_capability(),
        )
        if not bool(resp.get("accepted", resp.get("ok", False))):
            if bool(resp.get("reset_required", False)):
                reason = str(resp.get("reason", resp.get("error", "")) or "node_instance_id fenced")
                logger.warning(
                    "[Registrar] node register reset required node_id=%s node_instance_id=%s reason=%s",
                    self.node_id,
                    self.node_instance_id,
                    reason,
                )
                self._reset_state_after_fence(reason)
            return False
        with self._sync_lock:
            self._registered = True
            self._next_hb_sec = max(1, int(resp.get("heartbeat_interval_sec", self.fallback_heartbeat_sec) or self.fallback_heartbeat_sec))
            self._lease_ttl_sec = max(1, int(resp.get("lease_ttl_sec", self._lease_ttl_sec) or self._lease_ttl_sec))
            self._last_successful_sync_at = time.monotonic()
        logger.info(
            "[Registrar] node register node_id=%s node_instance_id=%s control_addr=%s hb=%s service_count=%d task_pool_count=%d",
            self.node_id,
            self.node_instance_id,
            self.control_addr,
            self._next_hb_sec,
            len(self.state.service_report_payloads(include_stopped=True)),
            len(self.state.task_pool_reports()),
        )
        return True

    def _heartbeat_once(self) -> bool:
        metrics = self.state.metrics()
        metadata = dict(self.metadata)
        metadata.update(self.state.service_timing_metadata())
        metadata["pycloud_version"] = self._pycloud_version()
        accept_service_deploy = bool(getattr(self.state, "accept_service_deploy", True))
        metadata["accept_service_deploy"] = "true" if accept_service_deploy else "false"
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
                "failure_reason": str(getattr(item, "failure_reason", "") or ""),
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
            services=self.state.service_report_payloads(include_stopped=True),
            task_pools=task_pools,
            active_runtimes=self.state.active_runtime_keys(limit=10),
            service_worker_capacity=self.state.service_worker_capacity,
            service_worker_used=self.state.service_worker_used(),
            task_pool_worker_capacity=self.state.task_pool_worker_capacity,
            task_pool_worker_used=self.state.task_pool_worker_used(),
            accept_service_deploy=accept_service_deploy,
            python_version=self.state.python_version,
            capability=self.state.node_capability(),
        )
        with self._sync_lock:
            if not resp.get("accepted", False):
                self._registered = False
                logger.warning(
                    "[Registrar] node heartbeat rejected node_id=%s node_instance_id=%s reset_required=%s reason=%s",
                    self.node_id,
                    self.node_instance_id,
                    bool(resp.get("reset_required", False)),
                    str(resp.get("reason", resp.get("error", "")) or ""),
                )
                if bool(resp.get("reset_required", False)):
                    self._reset_state_after_fence(str(resp.get("reason", resp.get("error", "")) or "node_instance_id fenced"))
                return False
            self._next_hb_sec = max(1, int(resp.get("next_heartbeat_in_sec", self.fallback_heartbeat_sec) or self.fallback_heartbeat_sec))
            self._lease_ttl_sec = max(1, int(resp.get("lease_ttl_sec", self._lease_ttl_sec) or self._lease_ttl_sec))
            self._last_successful_sync_at = time.monotonic()
        return True

    def _loop(self) -> None:
        while not self._stop_event.is_set():
            self.sync_now()
            with self._sync_lock:
                wait_sec = self._next_hb_sec if self._registered else self.fallback_heartbeat_sec
            self._wake_event.wait(max(1, int(wait_sec)))
            self._wake_event.clear()
