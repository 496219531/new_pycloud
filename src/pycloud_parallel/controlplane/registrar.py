from __future__ import annotations

"""Background registrar for NodeControl -> InfoCenter heartbeats."""

import logging
import os
import sys
import threading
import time
import uuid
from typing import Callable, Dict, Iterable, Optional
from importlib import metadata as importlib_metadata
from urllib.error import URLError

from pycloud_parallel.controlplane.infocenter_client import InfoCenterClient
from pycloud_parallel.controlplane.nodecontrol_state import NodeControlState

logger = logging.getLogger(__name__)
MAX_RECENT_INACTIVE_TASK_POOL_REPORTS = 32


def _env_bool(name: str, default: bool) -> bool:
    value = str(os.getenv(name, "") or "").strip().lower()
    if not value:
        return bool(default)
    return value not in {"0", "false", "no", "off"}


def _is_expected_connect_failure(exc: BaseException) -> bool:
    if isinstance(exc, (ConnectionError, TimeoutError, OSError)):
        return True
    if isinstance(exc, URLError):
        reason = getattr(exc, "reason", None)
        return reason is None or isinstance(reason, (ConnectionError, TimeoutError, OSError))
    if isinstance(exc, RuntimeError):
        text = str(exc or "").strip().lower()
        return any(
            marker in text
            for marker in (
                "cannot connect to ",
                "connection refused",
                "connection reset",
                "connection to ",
                "closed by the remote service",
                "http request to ",
                "timed out",
                "temporarily unavailable",
            )
        )
    return False


def _is_unknown_node_error(exc: BaseException) -> bool:
    current: Optional[BaseException] = exc
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        text = str(current or "").strip().lower()
        if "unknown node" in text:
            return True
        current = current.__cause__ or current.__context__
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


def _task_pool_report_sort_ts(item: object) -> float:
    for attr in ("last_heartbeat_at", "lease_expire_at", "created_at"):
        value = getattr(item, attr, None)
        timestamp = getattr(value, "timestamp", None)
        if callable(timestamp):
            try:
                return float(timestamp())
            except Exception:
                continue
    return 0.0


def _limit_task_pool_reports(
    reports: Iterable[object],
    *,
    inactive_limit: int = MAX_RECENT_INACTIVE_TASK_POOL_REPORTS,
) -> list[object]:
    running: list[object] = []
    inactive: list[object] = []
    for item in list(reports or []):
        if str(getattr(item, "status", "") or "").strip().upper() == "RUNNING":
            running.append(item)
        else:
            inactive.append(item)
    inactive.sort(key=_task_pool_report_sort_ts, reverse=True)
    return [*running, *inactive[: max(0, int(inactive_limit or 0))]]


def _exit_current_process_delayed(delay_sec: float = 0.25) -> None:
    def _exit() -> None:
        time.sleep(max(0.0, float(delay_sec or 0.0)))
        os._exit(0)

    threading.Thread(target=_exit, name="node-registrar-fence-exit", daemon=True).start()


def _restart_current_process_delayed(delay_sec: float = 0.25) -> None:
    def _restart() -> None:
        time.sleep(max(0.0, float(delay_sec or 0.0)))
        os.execv(sys.executable, [sys.executable, *sys.argv])

    threading.Thread(target=_restart, name="node-registrar-fence-restart", daemon=True).start()


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
        inventory_sync_interval_sec: float = 30.0,
        exit_on_fence: Optional[bool] = None,
        exit_delay_sec: Optional[float] = None,
        exit_callback: Optional[Callable[[float], None]] = None,
        restart_on_fence: Optional[bool] = None,
        restart_delay_sec: float = 1.0,
        restart_callback: Optional[Callable[[float], None]] = None,
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
        self.inventory_sync_interval_sec = max(1.0, float(inventory_sync_interval_sec or 30.0))
        self.restart_on_fence = (
            _env_bool("PYCLOUD_NODE_RESTART_ON_FENCE", False)
            if restart_on_fence is None
            else bool(restart_on_fence)
        )
        if exit_on_fence is None:
            exit_on_fence = _env_bool("PYCLOUD_NODE_EXIT_ON_FENCE", self.restart_on_fence)
        self.exit_on_fence = bool(exit_on_fence)
        self.exit_delay_sec = max(0.0, float(exit_delay_sec if exit_delay_sec is not None else restart_delay_sec or 0.25))
        self._exit_callback = exit_callback or _exit_current_process_delayed
        self._restart_callback = restart_callback or _restart_current_process_delayed

        self._client = InfoCenterClient(self.infocenter_addr, timeout_sec=self.rpc_timeout_sec)
        self._stop_event = threading.Event()
        self._wake_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._registered = False
        self._next_hb_sec = self.fallback_heartbeat_sec
        self._lease_ttl_sec = max(self.fallback_heartbeat_sec * 3, self.fallback_heartbeat_sec + 1)
        self._last_successful_sync_at = 0.0
        self._last_inventory_sync_at = 0.0
        self._force_inventory_sync = True
        self._sync_lock = threading.RLock()
        self._closing = False
        self._state_closed_after_lost = False

    @staticmethod
    def _pycloud_version() -> str:
        return _pycloud_version()

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        if getattr(self.state, "_infocenter_registrar", None) is None:
            setattr(self.state, "_infocenter_registrar", self)
        self._stop_event.clear()
        self._wake_event.clear()
        self._thread = threading.Thread(target=self._loop, name=f"node-registrar-{self.node_id}", daemon=True)
        self._thread.start()

    def close(self, *, mark_lost: bool = True) -> None:
        if self._closing:
            return
        self._closing = True
        try:
            if bool(mark_lost):
                self._mark_lost_before_close()
        finally:
            self._closing = False
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
        if getattr(self.state, "_infocenter_registrar", None) is self:
            setattr(self.state, "_infocenter_registrar", None)

    def _mark_lost_before_close(self) -> None:
        with self._sync_lock:
            was_registered = bool(self._registered)
        if not was_registered:
            return
        try:
            close_state = getattr(self.state, "close", None)
            if callable(close_state):
                previous_registrar = getattr(self.state, "_infocenter_registrar", None)
                if previous_registrar is self:
                    setattr(self.state, "_infocenter_registrar", None)
                close_state()
                if previous_registrar is self and getattr(self.state, "_infocenter_registrar", None) is None:
                    setattr(self.state, "_infocenter_registrar", self)
        except Exception:
            logger.exception(
                "[Registrar] state close before unregister failed node_id=%s node_instance_id=%s",
                self.node_id,
                self.node_instance_id,
            )
        try:
            self._heartbeat_once(force_inventory=True)
        except Exception as exc:
            if _is_expected_connect_failure(exc):
                logger.warning(
                    "[Registrar] final unregister heartbeat deferred node_id=%s node_instance_id=%s error=%s",
                    self.node_id,
                    self.node_instance_id,
                    _error_summary(exc),
                )
            else:
                logger.exception(
                    "[Registrar] final unregister heartbeat failed node_id=%s node_instance_id=%s",
                    self.node_id,
                    self.node_instance_id,
                )
        finally:
            with self._sync_lock:
                self._registered = False

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
            if ok:
                self._state_closed_after_lost = False
            return ok
        except Exception as exc:
            if was_registered and _is_unknown_node_error(exc):
                logger.warning(
                    "[Registrar] node heartbeat target missing; re-registering node_id=%s node_instance_id=%s "
                    "infocenter=%s control_addr=%s error=%s",
                    self.node_id,
                    self.node_instance_id,
                    self.infocenter_addr,
                    self.control_addr or "-",
                    _error_summary(exc),
                )
                with self._sync_lock:
                    self._registered = False
                try:
                    ok = self._register_once()
                    if ok:
                        self._state_closed_after_lost = False
                    return ok
                except Exception as register_exc:
                    if _is_expected_connect_failure(register_exc):
                        logger.warning(
                            "[Registrar] node re-register after missing heartbeat target deferred "
                            "node_id=%s node_instance_id=%s error=%s",
                            self.node_id,
                            self.node_instance_id,
                            _error_summary(register_exc),
                        )
                        logger.debug("[Registrar] node re-register traceback", exc_info=True)
                    else:
                        logger.exception(
                            "[Registrar] node re-register after missing heartbeat target failed "
                            "node_id=%s node_instance_id=%s",
                            self.node_id,
                            self.node_instance_id,
                        )
                    if _is_expected_connect_failure(register_exc):
                        self._mark_registration_stale_if_lease_expired("infocenter re-register lease expired")
                    else:
                        self._self_fence_if_lease_expired("infocenter re-register lease expired")
                        self._close_state_if_registration_lost(_error_summary(register_exc))
                    return False
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
            if _is_expected_connect_failure(exc):
                self._mark_registration_stale_if_lease_expired("infocenter heartbeat lease expired")
            else:
                self._self_fence_if_lease_expired("infocenter heartbeat lease expired")
                self._close_state_if_registration_lost(_error_summary(exc))
            return False

    def request_sync(self) -> None:
        with self._sync_lock:
            self._force_inventory_sync = True
        self._wake_event.set()

    def _close_state_if_registration_lost(self, reason: str) -> None:
        if not bool(getattr(self.state, "close_on_registration_lost", False)):
            return
        with self._sync_lock:
            if bool(self._registered):
                return
            if bool(self._state_closed_after_lost):
                return
            self._state_closed_after_lost = True
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

    def _reset_state_after_fence(self, reason: str, *, exit_host: Optional[bool] = None, restart: Optional[bool] = None, close_lost: bool = True) -> None:
        if exit_host is None:
            exit_host = True if restart is None else bool(restart)
        reset = getattr(self.state, "reset_execution_state", None)
        try:
            if callable(reset):
                reset(reason=reason or "node_instance_id fenced")
        finally:
            with self._sync_lock:
                self._registered = False
            self._stop_event.set()
            self._wake_event.set()
        if close_lost:
            self._close_state_if_registration_lost(reason or "node_instance_id fenced")
        should_exit_or_restart = self.exit_on_fence and bool(exit_host)
        fence_reason = f"{str(reason or 'node_instance_id fenced')} new_instance_required=True"
        if should_exit_or_restart and self.restart_on_fence:
            logger.warning(
                "[Registrar] restarting NodeControl host after fence node_id=%s node_instance_id=%s delay_sec=%.3f reason=%s",
                self.node_id,
                self.node_instance_id,
                self.exit_delay_sec,
                fence_reason,
            )
            self._restart_callback(self.exit_delay_sec)
        elif should_exit_or_restart:
            logger.warning(
                "[Registrar] exiting NodeControl host after fence node_id=%s node_instance_id=%s delay_sec=%.3f reason=%s",
                self.node_id,
                self.node_instance_id,
                self.exit_delay_sec,
                fence_reason,
            )
            self._exit_callback(self.exit_delay_sec)
        else:
            logger.warning(
                "[Registrar] NodeControl host exit after fence skipped node_id=%s node_instance_id=%s reason=%s",
                self.node_id,
                self.node_instance_id,
                fence_reason,
            )

    def _self_fence_if_lease_expired(self, reason: str) -> bool:
        lease_expired, detailed_reason, elapsed_since_success, lease_ttl = self._registration_lease_expiry_state(reason)
        if not lease_expired:
            return False
        logger.warning(
            "[Registrar] node self fence node_id=%s node_instance_id=%s "
            "infocenter=%s control_addr=%s last_success_age_sec=%.3f lease_ttl_sec=%.3f "
            "fallback_heartbeat_sec=%s next_heartbeat_sec=%s rpc_timeout_sec=%.3f reason=%s",
            self.node_id,
            self.node_instance_id,
            self.infocenter_addr,
            self.control_addr or "-",
            elapsed_since_success,
            lease_ttl,
            self.fallback_heartbeat_sec,
            self._next_hb_sec,
            self.rpc_timeout_sec,
            str(reason or "infocenter heartbeat lease expired"),
        )
        self._reset_state_after_fence(detailed_reason)
        return True

    def _mark_registration_stale_if_lease_expired(self, reason: str) -> bool:
        lease_expired, detailed_reason, elapsed_since_success, lease_ttl = self._registration_lease_expiry_state(reason)
        if not lease_expired:
            return False
        logger.warning(
            "[Registrar] node registration stale; keeping local runtime alive for re-register "
            "node_id=%s node_instance_id=%s infocenter=%s control_addr=%s "
            "last_success_age_sec=%.3f lease_ttl_sec=%.3f fallback_heartbeat_sec=%s "
            "next_heartbeat_sec=%s rpc_timeout_sec=%.3f reason=%s",
            self.node_id,
            self.node_instance_id,
            self.infocenter_addr,
            self.control_addr or "-",
            elapsed_since_success,
            lease_ttl,
            self.fallback_heartbeat_sec,
            self._next_hb_sec,
            self.rpc_timeout_sec,
            str(reason or "infocenter heartbeat lease expired"),
        )
        logger.debug("[Registrar] stale registration detail: %s", detailed_reason)
        return True

    def _registration_lease_expiry_state(self, reason: str) -> tuple[bool, str, float, float]:
        with self._sync_lock:
            last_success = float(self._last_successful_sync_at or 0.0)
            lease_ttl = max(1.0, float(self._lease_ttl_sec or self.fallback_heartbeat_sec))
            now_monotonic = time.monotonic()
            elapsed_since_success = now_monotonic - last_success if last_success else 0.0
            if not last_success or elapsed_since_success <= lease_ttl:
                return False, "", elapsed_since_success, lease_ttl
            self._registered = False
        detailed_reason = (
            f"{reason or 'infocenter heartbeat lease expired'}; "
            f"infocenter={self.infocenter_addr} control_addr={self.control_addr or '-'} "
            f"last_success_age_sec={elapsed_since_success:.3f} lease_ttl_sec={lease_ttl:.3f} "
            f"fallback_heartbeat_sec={self.fallback_heartbeat_sec} next_heartbeat_sec={self._next_hb_sec} "
            f"rpc_timeout_sec={self.rpc_timeout_sec}"
        )
        return True, detailed_reason, elapsed_since_success, lease_ttl

    def _register_once(self) -> bool:
        snapshot = self.state.registrar_snapshot(include_stopped=True, runtime_limit=10, include_inventory=True)
        metadata = dict(self.metadata)
        metadata.update(snapshot.get("service_timing_metadata", {}))
        metadata["pycloud_version"] = self._pycloud_version()
        accept_service_deploy = bool(snapshot.get("accept_service_deploy", getattr(self.state, "accept_service_deploy", True)))
        metadata["accept_service_deploy"] = "true" if accept_service_deploy else "false"
        if bool(snapshot.get("execution_fenced", False)):
            metadata["execution_fenced"] = "true"
        deploy_health_reason = str(snapshot.get("deploy_health_reason", "") or "").strip()
        if deploy_health_reason:
            metadata["deploy_health_reason"] = deploy_health_reason
        task_pool_reports = _limit_task_pool_reports(snapshot.get("task_pool_reports") or [])
        service_reports = list(snapshot.get("service_reports") or [])
        active_runtimes = list(snapshot.get("active_runtimes") or [])
        task_pools = [
            {
                "pool_id": item.pool_id,
                "owner_client_id": item.owner_client_id,
                "pool_name": item.pool_name,
                "code_version": item.code_version,
                "status": item.status,
                "resource_health": str(getattr(item, "resource_health", "") or ""),
                "degraded": bool(getattr(item, "degraded", False)),
                "worker_count": int(item.worker_count),
                "alive_workers": int(getattr(item, "alive_workers", 0) or 0),
                "task_count": int(item.task_count),
                "inflight": int(item.inflight),
                "received_count": int(getattr(item, "received_count", item.task_count) or 0),
                "returned_count": int(getattr(item, "returned_count", 0) or 0),
                "stop_reason": str(getattr(item, "stop_reason", getattr(item, "failure_reason", "")) or ""),
                "failure_reason": str(getattr(item, "failure_reason", "") or ""),
                "failure_at": item.failure_at.isoformat() if getattr(item, "failure_at", None) is not None else "",
                "created_at": item.created_at.isoformat(),
                "last_heartbeat_at": item.last_heartbeat_at.isoformat(),
                "lease_expire_at": item.lease_expire_at.isoformat(),
            }
            for item in task_pool_reports
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
            services=service_reports,
            task_pools=task_pools,
            active_runtimes=active_runtimes,
            service_worker_capacity=self.state.service_worker_capacity,
            service_worker_used=int(snapshot.get("service_worker_used", 0) or 0),
            task_pool_worker_capacity=self.state.task_pool_worker_capacity,
            task_pool_worker_used=int(snapshot.get("task_pool_worker_used", 0) or 0),
            accept_service_deploy=accept_service_deploy,
            python_version=self.state.python_version,
            capability=self.state.node_capability(),
        )
        if not bool(resp.get("accepted", resp.get("ok", False))):
            if bool(resp.get("reset_required", False)):
                reason = str(resp.get("reason", resp.get("error", "")) or "node_instance_id fenced")
                logger.warning(
                    "[Registrar] node register reset required node_id=%s node_instance_id=%s "
                    "infocenter=%s control_addr=%s reason=%s response=%s",
                    self.node_id,
                    self.node_instance_id,
                    self.infocenter_addr,
                    self.control_addr or "-",
                    reason,
                    resp,
                )
                self._reset_state_after_fence(
                    f"register reset required; infocenter={self.infocenter_addr} "
                    f"control_addr={self.control_addr or '-'} reason={reason}",
                )
            return False
        with self._sync_lock:
            self._registered = True
            self._next_hb_sec = max(1, int(resp.get("heartbeat_interval_sec", self.fallback_heartbeat_sec) or self.fallback_heartbeat_sec))
            self._lease_ttl_sec = max(1, int(resp.get("lease_ttl_sec", self._lease_ttl_sec) or self._lease_ttl_sec))
            self._last_successful_sync_at = time.monotonic()
            self._last_inventory_sync_at = self._last_successful_sync_at
            self._force_inventory_sync = False
        logger.info(
            "[Registrar] node register node_id=%s node_instance_id=%s control_addr=%s hb=%s service_count=%d task_pool_count=%d",
            self.node_id,
            self.node_instance_id,
            self.control_addr,
            self._next_hb_sec,
            len(service_reports),
            len(task_pool_reports),
        )
        return True

    def _heartbeat_once(self, *, force_inventory: bool = False) -> bool:
        now_monotonic = time.monotonic()
        with self._sync_lock:
            include_inventory = bool(force_inventory) or bool(self._force_inventory_sync) or (
                now_monotonic - float(self._last_inventory_sync_at or 0.0) >= self.inventory_sync_interval_sec
            )
        snapshot = self.state.registrar_snapshot(
            include_stopped=True,
            runtime_limit=10,
            include_inventory=include_inventory,
        )
        metrics = dict(snapshot.get("metrics") or {})
        metadata = dict(self.metadata)
        metadata.update(snapshot.get("service_timing_metadata", {}))
        metadata["pycloud_version"] = self._pycloud_version()
        accept_service_deploy = bool(snapshot.get("accept_service_deploy", getattr(self.state, "accept_service_deploy", True)))
        metadata["accept_service_deploy"] = "true" if accept_service_deploy else "false"
        if bool(snapshot.get("execution_fenced", False)):
            metadata["execution_fenced"] = "true"
        deploy_health_reason = str(snapshot.get("deploy_health_reason", "") or "").strip()
        if deploy_health_reason:
            metadata["deploy_health_reason"] = deploy_health_reason
        task_pool_reports = _limit_task_pool_reports(snapshot.get("task_pool_reports") or [])
        service_reports = list(snapshot.get("service_reports") or [])
        active_runtimes = list(snapshot.get("active_runtimes") or [])
        task_pools = [
            {
                "pool_id": item.pool_id,
                "owner_client_id": item.owner_client_id,
                "pool_name": item.pool_name,
                "code_version": item.code_version,
                "status": item.status,
                "resource_health": str(getattr(item, "resource_health", "") or ""),
                "degraded": bool(getattr(item, "degraded", False)),
                "worker_count": int(item.worker_count),
                "alive_workers": int(getattr(item, "alive_workers", 0) or 0),
                "task_count": int(item.task_count),
                "inflight": int(item.inflight),
                "received_count": int(getattr(item, "received_count", item.task_count) or 0),
                "returned_count": int(getattr(item, "returned_count", 0) or 0),
                "stop_reason": str(getattr(item, "stop_reason", getattr(item, "failure_reason", "")) or ""),
                "failure_reason": str(getattr(item, "failure_reason", "") or ""),
                "failure_at": item.failure_at.isoformat() if getattr(item, "failure_at", None) is not None else "",
                "created_at": item.created_at.isoformat(),
                "last_heartbeat_at": item.last_heartbeat_at.isoformat(),
                "lease_expire_at": item.lease_expire_at.isoformat(),
            }
            for item in task_pool_reports
        ]
        resp = self._client.heartbeat_node(
            node_id=self.node_id,
            node_instance_id=self.node_instance_id,
            healthy=not bool(snapshot.get("execution_fenced", False)),
            metrics={
                "queued": metrics["queued"],
                "inflight": metrics["inflight"],
                "running": metrics["running"],
                "credit": metrics["credit"],
                "cpu_percent": 0.0,
                "mem_percent": 0.0,
            },
            metadata=metadata,
            services=service_reports,
            task_pools=task_pools,
            active_runtimes=active_runtimes,
            inventory_included=include_inventory,
            service_worker_capacity=self.state.service_worker_capacity,
            service_worker_used=int(snapshot.get("service_worker_used", 0) or 0),
            task_pool_worker_capacity=self.state.task_pool_worker_capacity,
            task_pool_worker_used=int(snapshot.get("task_pool_worker_used", 0) or 0),
            accept_service_deploy=accept_service_deploy,
            python_version=self.state.python_version,
            capability=self.state.node_capability(),
        )
        with self._sync_lock:
            if not resp.get("accepted", False):
                self._registered = False
                logger.warning(
                    "[Registrar] node heartbeat rejected node_id=%s node_instance_id=%s "
                    "infocenter=%s control_addr=%s reset_required=%s reason=%s response=%s",
                    self.node_id,
                    self.node_instance_id,
                    self.infocenter_addr,
                    self.control_addr or "-",
                    bool(resp.get("reset_required", False)),
                    str(resp.get("reason", resp.get("error", "")) or ""),
                    resp,
                )
                if bool(resp.get("reset_required", False)):
                    reason = str(resp.get("reason", resp.get("error", "")) or "node_instance_id fenced")
                    self._reset_state_after_fence(
                        f"heartbeat reset required; infocenter={self.infocenter_addr} "
                        f"control_addr={self.control_addr or '-'} reason={reason}",
                    )
                return False
            self._next_hb_sec = max(1, int(resp.get("next_heartbeat_in_sec", self.fallback_heartbeat_sec) or self.fallback_heartbeat_sec))
            self._lease_ttl_sec = max(1, int(resp.get("lease_ttl_sec", self._lease_ttl_sec) or self._lease_ttl_sec))
            self._last_successful_sync_at = time.monotonic()
            if include_inventory:
                self._last_inventory_sync_at = self._last_successful_sync_at
                self._force_inventory_sync = False
        return True

    def _loop(self) -> None:
        while not self._stop_event.is_set():
            self.sync_now()
            with self._sync_lock:
                wait_sec = self._next_hb_sec if self._registered else self.fallback_heartbeat_sec
            self._wake_event.wait(max(1, int(wait_sec)))
            self._wake_event.clear()
