from __future__ import annotations

"""Shared execution session base classes for the authoritative V1 execution layer."""

import contextlib
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
import json
import logging
import threading
import time
from typing import Any, Dict, Optional, Tuple, Union
from urllib.parse import quote
from urllib.request import Request, urlopen

from pycloud_parallel.controlplane.infocenter_client import InfoCenterNode
from pycloud_parallel.controlplane.session_handle import ExecutionReplicaHandle
from pycloud_parallel.controlplane.session_model import (
    ExecutionReplicaSnapshot,
    ExecutionSessionStatus,
    SessionLease,
    build_execution_session_status,
)
from pycloud_parallel.execution.error_classifier import ErrorCategory, classify_error, is_terminal_heartbeat_error
from pycloud_parallel.execution.recovery_state import ReplicaRecoveryState, build_replica_recovery_state
from pycloud_parallel.proto.v1 import pycloud_v1_pb2 as pb2


logger = logging.getLogger(__name__)
SLOW_HEARTBEAT_LOG_SEC = 2.0
STATUS_LOG_FETCH_TIMEOUT_SEC = 1.0


@dataclass(frozen=True)
class ExecutionItem:
    index: int = -1
    ok: bool = False
    result: Any = None
    error_type: str = ""
    error_message: str = ""
    node_id: str = ""
    key: Union[int, str] = -1
    status: int = 0
    task_id: str = ""
    node_instance_id: str = ""

    @property
    def data(self) -> Any:
        return self.result


class ExecutionSessionBase:
    kind: str = ""
    nodes: Dict[str, InfoCenterNode]
    failures: Dict[str, str]
    globals_digests: Dict[str, str]

    def _replica_handles(self) -> Dict[str, ExecutionReplicaHandle]:
        raise NotImplementedError

    def _init_execution_session_state(self) -> None:
        self._hb_stop = threading.Event()
        self._hb_thread = None
        self._hb_lock = threading.Lock()
        self._keepalive_seq = 0
        self._keepalive_failure_counts = {}
        self._keepalive_retry_forever = bool(getattr(self, "_keepalive_retry_forever", False))
        self._active_replica_lock = threading.RLock()
        self._active_replica_ids = set(self.replicas.keys())
        self._terminal_replica_ids = set()
        self._retry_probe_replica_ids = set()
        if hasattr(self, "_active_nodes"):
            self._active_nodes = self._active_replica_ids
        if not hasattr(self, "failed"):
            self.failed = False

    @property
    def replicas(self) -> Dict[str, ExecutionReplicaHandle]:
        return self._replica_handles()

    def snapshot(self) -> Dict[str, ExecutionReplicaSnapshot]:
        snapshots: Dict[str, ExecutionReplicaSnapshot] = {}
        for node_instance_id, replica in self.replicas.items():
            node = self.nodes.get(node_instance_id)
            snapshots[node_instance_id] = replica.snapshot(
                node_instance_id=node_instance_id,
                node_id=str(node.node_id if node is not None else getattr(replica, "node_id", "") or ""),
                failure=str(self.failures.get(node_instance_id, "") or ""),
            )
        return snapshots

    def _replica_leases(self) -> Dict[str, SessionLease]:
        leases: Dict[str, SessionLease] = {}
        for node_instance_id, replica in self.replicas.items():
            try:
                leases[node_instance_id] = replica.lease()
            except Exception:
                continue
        return leases

    def _is_execution_closed(self) -> bool:
        return bool(getattr(self, "_closed", False))

    def status(self) -> ExecutionSessionStatus:
        replicas = self.snapshot()
        leases = self._replica_leases()
        return build_execution_session_status(
            kind=str(getattr(self, "kind", "") or ""),
            replicas=replicas,
            failures=self.failures,
            failed=bool(getattr(self, "failed", False)),
            closed=self._is_execution_closed(),
            leases=leases,
        )

    @property
    def last_heartbeat_at(self):
        return self.status().last_heartbeat_at

    @property
    def lease_expire_at(self):
        return self.status().lease_expire_at

    def is_alive(self) -> bool:
        return self.status().alive

    def replica_recovery_states(self) -> Dict[str, ReplicaRecoveryState]:
        return self._build_replica_recovery_states()

    def _build_replica_recovery_states(
        self,
        *,
        is_retryable_failure: Optional[Any] = None,
    ) -> Dict[str, ReplicaRecoveryState]:
        active_ids = self._active_replica_snapshot()
        replica_ids = {str(node_id) for node_id in self.replicas.keys() if str(node_id)}
        replica_ids.update(str(node_id) for node_id in self.failures.keys() if str(node_id))

        def _is_retryable(node_id: str) -> bool:
            message = self.failures.get(node_id, "")
            if not message:
                return True
            if is_retryable_failure is None:
                return True
            return bool(is_retryable_failure(str(message or "")))

        return {
            str(node_id): build_replica_recovery_state(
                str(node_id),
                active=str(node_id) in active_ids,
                terminal=self._is_terminal_replica(str(node_id)),
                retryable=_is_retryable(str(node_id)),
                error=self.failures.get(str(node_id), ""),
            )
            for node_id in replica_ids
        }

    def _default_keepalive_interval_sec(self, interval_sec: Optional[float] = None) -> float:
        if interval_sec is not None:
            return max(0.05, float(interval_sec))
        timeouts = [
            max(1, int(getattr(replica, "heartbeat_timeout_sec", 0) or 1))
            for replica in self.replicas.values()
        ]
        if not timeouts:
            return 1.0
        return max(0.5, min(30.0, min(timeouts) / 2.0))

    def _heartbeat_failure_threshold(self, node_id: str, replica: ExecutionReplicaHandle) -> int:
        del node_id
        return max(1, int(getattr(replica, "heartbeat_failure_threshold", 1) or 1))

    def _heartbeat_max_workers(self) -> int:
        replica_count = max(1, len(self.replicas))
        if bool(getattr(self, "_keepalive_retry_forever", False)):
            return max(4, min(32, replica_count * 2))
        return max(1, min(32, replica_count))

    def _heartbeat_replica(self, node_id: str, replica: ExecutionReplicaHandle, *, seq: int) -> Any:
        del node_id
        try:
            return replica.heartbeat(seq=seq)
        except TypeError:
            return replica.heartbeat()

    def _replica_log_context(self, node_id: str, replica: ExecutionReplicaHandle) -> Dict[str, object]:
        context: Dict[str, object] = {
            "node_instance_id": str(node_id or ""),
            "node_id": str(getattr(replica, "node_id", "") or ""),
            "kind": str(getattr(replica, "kind", "") or self.kind or ""),
            "session_id": str(getattr(replica, "session_id", "") or ""),
            "session_name": str(getattr(replica, "session_name", "") or ""),
            "status": str(getattr(replica, "status", "") or ""),
            "heartbeat_timeout_sec": int(getattr(replica, "heartbeat_timeout_sec", 0) or 0),
            "last_error": str(getattr(replica, "last_error", "") or ""),
        }
        pool_id = str(getattr(replica, "pool_id", "") or "")
        pool_name = str(getattr(replica, "pool_name", "") or "")
        if pool_id:
            context["pool_id"] = pool_id
        if pool_name:
            context["pool_name"] = pool_name
        service_id = str(getattr(replica, "service_id", "") or "")
        service_name = str(getattr(replica, "service_name", "") or "")
        if service_id:
            context["service_id"] = service_id
        if service_name:
            context["service_name"] = service_name
        try:
            lease = replica.lease()
            context["last_heartbeat_at"] = getattr(lease, "last_heartbeat_at", "")
            context["lease_expire_at"] = getattr(lease, "lease_expire_at", "")
        except Exception as exc:
            context["lease_error"] = repr(exc)
        try:
            status = replica.get_status()
            context["remote_status"] = str(getattr(status, "status", "") or "")
            context["remote_task_count"] = int(getattr(status, "task_count", 0) or 0)
            context["remote_worker_count"] = int(getattr(status, "worker_count", 0) or 0)
            remote_pool_name = str(getattr(status, "pool_name", "") or "")
            if remote_pool_name:
                context["remote_pool_name"] = remote_pool_name
            remote_service_name = str(getattr(status, "service_name", "") or "")
            if remote_service_name:
                context["remote_service_name"] = remote_service_name
        except Exception as exc:
            context["remote_status_error"] = repr(exc)
            context.update(self._replica_http_status_context(replica))
        return context

    def _replica_http_status_context(self, replica: ExecutionReplicaHandle) -> Dict[str, object]:
        pool_id = str(getattr(replica, "pool_id", "") or "")
        client = getattr(replica, "_client", None)
        base_url = str(getattr(client, "base_url", "") or getattr(client, "target", "") or "").rstrip("/")
        if not pool_id or not base_url.lower().startswith(("http://", "https://")):
            return {}
        try:
            request = Request(f"{base_url}/taskpools/{quote(pool_id, safe='')}", method="GET")
            with urlopen(request, timeout=STATUS_LOG_FETCH_TIMEOUT_SEC) as response:  # noqa: S310
                raw = response.read(256 * 1024)
            payload = json.loads(raw.decode("utf-8"))
            pool = dict(payload.get("pool") or {})
        except Exception as exc:
            return {"remote_json_status_error": repr(exc)}
        out: Dict[str, object] = {}
        for key in (
            "status",
            "failure_reason",
            "received_count",
            "returned_count",
            "inflight",
            "alive_workers",
            "worker_count",
            "last_heartbeat_at",
            "lease_expire_at",
        ):
            if key in pool:
                out[f"remote_{key}"] = pool.get(key)
        return out

    def _timed_heartbeat_replica(self, node_id: str, replica: ExecutionReplicaHandle, *, seq: int) -> Any:
        started_at = time.monotonic()
        try:
            return self._heartbeat_replica(node_id, replica, seq=seq)
        finally:
            elapsed = time.monotonic() - started_at
            if elapsed >= SLOW_HEARTBEAT_LOG_SEC:
                logger.warning(
                    "%s keepalive heartbeat slow node_instance_id=%s elapsed_sec=%.3f",
                    self.kind or "execution",
                    node_id,
                    elapsed,
                )

    def _mark_replica_heartbeat_success(self, node_id: str, replica: ExecutionReplicaHandle) -> None:
        self._keepalive_failure_counts.pop(node_id, None)
        self._discard_terminal_replica(node_id)
        self._discard_retry_probe_replica(node_id)
        if hasattr(replica, "failed"):
            replica.failed = False
        if hasattr(replica, "last_error"):
            replica.last_error = ""
        self.failures.pop(node_id, None)
        self._add_active_replica(node_id)

    def _mark_replica_heartbeat_failure(self, node_id: str, replica: ExecutionReplicaHandle, exc: Exception) -> None:
        message = repr(exc)
        self.failures[node_id] = message
        if hasattr(replica, "failed"):
            replica.failed = True
        if hasattr(replica, "last_error"):
            replica.last_error = message
        if getattr(replica, "kind", "") == "service" and hasattr(replica, "status"):
            replica.status = pb2.SERVICE_STATUS_STOPPED
        elif getattr(replica, "kind", "") == "task_pool" and hasattr(replica, "status"):
            replica.status = "STOPPED"
        self._discard_active_replica(node_id)

    def _terminal_heartbeat_error_markers(self, replica: ExecutionReplicaHandle) -> Tuple[str, ...]:
        common_markers = (
            "node instance execution is fenced",
            "node_instance_id fenced",
            "control_addr replaced",
        )
        kind = str(getattr(replica, "kind", "") or "").strip()
        if kind == "task_pool":
            return common_markers + (
                "task pool not running",
                "task pool not found",
                "pool is stopped",
                "pool not found",
            )
        if kind == "service":
            return common_markers + (
                "service is stopped",
                "service not found",
            )
        return common_markers

    def _is_terminal_heartbeat_error(self, node_id: str, replica: ExecutionReplicaHandle, exc: Exception) -> bool:
        del node_id
        resource_kind = str(getattr(replica, "kind", "") or "").strip()
        return is_terminal_heartbeat_error(exc, resource_kind=resource_kind)

    def _is_retry_probe_heartbeat_error(self, node_id: str, replica: ExecutionReplicaHandle, exc: Exception) -> bool:
        del node_id, replica
        return classify_error(exc) == ErrorCategory.TRANSIENT_NETWORK

    def _mark_terminal_replica(self, node_id: str) -> None:
        lock = getattr(self, "_active_replica_lock", None)
        if lock is None:
            self._terminal_replica_ids.add(node_id)
            return
        with lock:
            self._terminal_replica_ids.add(node_id)

    def _discard_terminal_replica(self, node_id: str) -> None:
        lock = getattr(self, "_active_replica_lock", None)
        if lock is None:
            getattr(self, "_terminal_replica_ids", set()).discard(node_id)
            return
        with lock:
            getattr(self, "_terminal_replica_ids", set()).discard(node_id)

    def _is_terminal_replica(self, node_id: str) -> bool:
        lock = getattr(self, "_active_replica_lock", None)
        if lock is None:
            return node_id in getattr(self, "_terminal_replica_ids", set())
        with lock:
            return node_id in getattr(self, "_terminal_replica_ids", set())

    def _mark_retry_probe_replica(self, node_id: str) -> None:
        lock = getattr(self, "_active_replica_lock", None)
        if lock is None:
            getattr(self, "_retry_probe_replica_ids", set()).add(node_id)
            return
        with lock:
            getattr(self, "_retry_probe_replica_ids", set()).add(node_id)

    def _discard_retry_probe_replica(self, node_id: str) -> None:
        lock = getattr(self, "_active_replica_lock", None)
        if lock is None:
            getattr(self, "_retry_probe_replica_ids", set()).discard(node_id)
            return
        with lock:
            getattr(self, "_retry_probe_replica_ids", set()).discard(node_id)

    def _retry_probe_replica_snapshot(self) -> set[str]:
        lock = getattr(self, "_active_replica_lock", None)
        if lock is None:
            return {str(node_id) for node_id in list(getattr(self, "_retry_probe_replica_ids", set()) or []) if str(node_id)}
        with lock:
            return {str(node_id) for node_id in list(getattr(self, "_retry_probe_replica_ids", set()) or []) if str(node_id)}

    def _active_replica_snapshot(self) -> set[str]:
        lock = getattr(self, "_active_replica_lock", None)
        if lock is None:
            return {str(node_id) for node_id in list(getattr(self, "_active_replica_ids", set()) or []) if str(node_id)}
        with lock:
            return {str(node_id) for node_id in list(getattr(self, "_active_replica_ids", set()) or []) if str(node_id)}

    def _add_active_replica(self, node_id: str) -> None:
        lock = getattr(self, "_active_replica_lock", None)
        if lock is None:
            getattr(self, "_terminal_replica_ids", set()).discard(node_id)
            self._active_replica_ids.add(node_id)
            return
        with lock:
            getattr(self, "_terminal_replica_ids", set()).discard(node_id)
            self._active_replica_ids.add(node_id)

    def _discard_active_replica(self, node_id: str) -> None:
        lock = getattr(self, "_active_replica_lock", None)
        if lock is None:
            self._active_replica_ids.discard(node_id)
            return
        with lock:
            self._active_replica_ids.discard(node_id)

    def _record_heartbeat_failure(self, node_id: str, replica: ExecutionReplicaHandle, exc: Exception) -> None:
        count = int(self._keepalive_failure_counts.get(node_id, 0) or 0) + 1
        self._keepalive_failure_counts[node_id] = count
        if count >= self._heartbeat_failure_threshold(node_id, replica):
            self._mark_replica_heartbeat_failure(node_id, replica, exc)
        if self._is_retry_probe_heartbeat_error(node_id, replica, exc):
            self._mark_retry_probe_replica(node_id)

    def _record_terminal_heartbeat_failure(self, node_id: str, replica: ExecutionReplicaHandle, exc: Exception) -> None:
        self._keepalive_failure_counts[node_id] = self._heartbeat_failure_threshold(node_id, replica)
        self._mark_replica_heartbeat_failure(node_id, replica, exc)
        self._mark_terminal_replica(node_id)

    def _handle_heartbeat_exception(self, node_id: str, replica: ExecutionReplicaHandle, exc: Exception) -> None:
        if self._is_terminal_heartbeat_error(node_id, replica, exc):
            if not self._is_terminal_replica(node_id):
                logger.warning(
                    "%s keepalive replica stopped context=%s error=%r",
                    self.kind or "execution",
                    self._replica_log_context(node_id, replica),
                    exc,
                )
            self._record_terminal_heartbeat_failure(node_id, replica, exc)
            return
        logger.warning(
            "%s keepalive heartbeat failed context=%s error=%r",
            self.kind or "execution",
            self._replica_log_context(node_id, replica),
            exc,
        )
        self._record_heartbeat_failure(node_id, replica, exc)

    def _keepalive_loop(self, interval_sec: float) -> None:
        next_tick = time.monotonic() + max(0.1, float(interval_sec))
        pending: Dict[str, Tuple[Future, ExecutionReplicaHandle, float, float]] = {}
        executor = ThreadPoolExecutor(max_workers=self._heartbeat_max_workers(), thread_name_prefix=f"{self.kind or 'execution'}-hb")
        try:
            while not self._hb_stop.is_set():
                now = time.monotonic()
                for node_id, (future, replica, started_at, last_pending_report_at) in list(pending.items()):
                    if self.replicas.get(node_id) is not replica:
                        future.cancel()
                        pending.pop(node_id, None)
                        continue
                    if not future.done():
                        max_pending_sec = max(1.0, float(getattr(replica, "heartbeat_timeout_sec", 1) or 1))
                        if now - started_at >= max_pending_sec and now - last_pending_report_at >= max_pending_sec:
                            logger.warning(
                                "%s keepalive heartbeat pending node_instance_id=%s elapsed_sec=%.3f timeout_sec=%.3f",
                                self.kind or "execution",
                                node_id,
                                now - started_at,
                                max_pending_sec,
                            )
                            self._record_heartbeat_failure(
                                node_id,
                                replica,
                                TimeoutError(f"heartbeat pending for {now - started_at:.3f}s"),
                            )
                            future.cancel()
                            pending.pop(node_id, None)
                        continue
                    pending.pop(node_id, None)
                    try:
                        future.result()
                        self._mark_replica_heartbeat_success(node_id, replica)
                    except Exception as exc:
                        self._handle_heartbeat_exception(node_id, replica, exc)

                wait_sec = max(0.0, next_tick - time.monotonic())
                if self._hb_stop.wait(wait_sec):
                    break
                next_tick += max(0.1, float(interval_sec))
                self._keepalive_seq += 1
                replicas = self.replicas
                heartbeat_ids = list(self._active_replica_snapshot())
                if bool(getattr(self, "_keepalive_retry_forever", False)):
                    heartbeat_ids = list(dict.fromkeys([*heartbeat_ids, *list(replicas.keys())]))
                else:
                    heartbeat_ids = list(dict.fromkeys([*heartbeat_ids, *list(self._retry_probe_replica_snapshot())]))
                heartbeat_ids = [node_id for node_id in heartbeat_ids if not self._is_terminal_replica(node_id)]
                for node_id in heartbeat_ids:
                    if node_id in pending:
                        continue
                    replica = replicas.get(node_id)
                    if replica is None:
                        self._discard_active_replica(node_id)
                        continue
                    pending[node_id] = (
                        executor.submit(self._timed_heartbeat_replica, node_id, replica, seq=self._keepalive_seq),
                        replica,
                        time.monotonic(),
                        time.monotonic(),
                    )
                if not self._active_replica_snapshot():
                    retryable_replica_ids = [
                        str(node_id)
                        for node_id in self.replicas.keys()
                        if (
                            str(node_id)
                            and not self._is_terminal_replica(str(node_id))
                            and (
                                bool(getattr(self, "_keepalive_retry_forever", False))
                                or str(node_id) in self._retry_probe_replica_snapshot()
                            )
                        )
                    ]
                    can_retry = bool(retryable_replica_ids)
                    can_compensate = bool(getattr(self, "_compensation_spec", None))
                    if not (can_retry or can_compensate):
                        self.failed = True
                        self._hb_stop.set()
                        break
                hook = getattr(self, "_after_keepalive_tick", None)
                if callable(hook):
                    with contextlib.suppress(Exception):
                        hook()
            for future, _replica, _started_at, _last_pending_report_at in pending.values():
                future.cancel()
        finally:
            executor.shutdown(wait=False, cancel_futures=True)

    def _start_keepalive(self, interval_sec: Optional[float] = None) -> None:
        with self._hb_lock:
            if self._hb_thread is not None and self._hb_thread.is_alive():
                return
            self.failed = False
            self._keepalive_failure_counts = {}
            self._active_replica_lock = getattr(self, "_active_replica_lock", threading.RLock())
            with self._active_replica_lock:
                self._active_replica_ids = set(self.replicas.keys())
                self._terminal_replica_ids = set()
                self._retry_probe_replica_ids = set()
            if hasattr(self, "_active_nodes"):
                self._active_nodes = self._active_replica_ids
            for replica in self.replicas.values():
                if hasattr(replica, "failed"):
                    replica.failed = False
                if hasattr(replica, "last_error"):
                    replica.last_error = ""
            self._hb_stop.clear()
            wait_sec = self._default_keepalive_interval_sec(interval_sec)
            self._hb_thread = threading.Thread(
                target=self._keepalive_loop,
                args=(wait_sec,),
                name=f"{self.kind or 'execution'}-hb",
                daemon=True,
            )
            self._hb_thread.start()
            for replica in self.replicas.values():
                setattr(replica, "_hb_thread", self._hb_thread)
                setattr(replica, "_hb_lock", self._hb_lock)

    def _stop_keepalive(self) -> None:
        with self._hb_lock:
            self._hb_stop.set()
            thread = self._hb_thread
        if thread is not None:
            thread.join(timeout=2.0)
        with self._hb_lock:
            self._hb_thread = None
            for replica in self.replicas.values():
                setattr(replica, "_hb_thread", None)
                setattr(replica, "_hb_lock", self._hb_lock)

    def _sync_failures_from_replicas(self) -> None:
        for node_id, replica in self.replicas.items():
            if getattr(replica, "failed", False):
                self.failures[node_id] = str(
                    getattr(replica, "last_error", "") or self.failures.get(node_id, "") or "replica failed"
                )


class ServiceExecutionSession(ExecutionSessionBase):
    kind = "service"


class TaskExecutionSession(ExecutionSessionBase):
    kind = "task_pool"


__all__ = ["ExecutionItem", "ExecutionSessionBase", "ServiceExecutionSession", "TaskExecutionSession"]
