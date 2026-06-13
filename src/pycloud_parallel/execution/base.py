from __future__ import annotations

"""Shared execution session base classes for the authoritative V1 execution layer."""

import contextlib
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from enum import Enum
import json
import logging
import os
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
SLOW_COMPENSATION_LOG_SEC = 2.0
STATUS_LOG_FETCH_TIMEOUT_SEC = 1.0
STALE_HEARTBEAT_PENDING_PER_REPLICA = 2
HEARTBEAT_DEFAULT_MAX_WORKERS = 256
HEARTBEAT_DEFAULT_RPC_TIMEOUT_SEC = 2.0
HEARTBEAT_MIN_RPC_TIMEOUT_SEC = 0.05
HEARTBEAT_MIN_QUEUE_TIMEOUT_SEC = 0.2
OWNER_ZERO_ALIVE_DISCONNECT_SEC = 100.0
OWNER_ZERO_ALIVE_BLACKLIST_WINDOW_SEC = 3600.0


def _coerce_positive_float(value: Any, default: float = 0.0) -> float:
    try:
        parsed = float(value or 0.0)
    except (TypeError, ValueError):
        return float(default)
    return parsed if parsed > 0.0 else float(default)


def _coerce_positive_int(value: Any, default: int = 0) -> int:
    try:
        parsed = int(value or 0)
    except (TypeError, ValueError):
        return int(default)
    return parsed if parsed > 0 else int(default)


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


@dataclass
class _HeartbeatPending:
    replica: ExecutionReplicaHandle
    submitted_at: float
    last_pending_report_at: float
    future: Optional[Future] = None
    started_at: float = 0.0


class HeartbeatErrorKind(str, Enum):
    SUCCESS = "success"
    TRANSIENT = "transient"
    TERMINAL = "terminal"
    UNKNOWN = "unknown"


class ExecutionSessionBase:
    kind: str = ""
    nodes: Dict[str, InfoCenterNode]
    failures: Dict[str, str]
    globals_digests: Dict[str, str]

    def _replica_handles(self) -> Dict[str, ExecutionReplicaHandle]:
        raise NotImplementedError

    def _init_execution_session_state(self) -> None:
        self._hb_stop = threading.Event()
        self._hb_wakeup = threading.Event()
        self._hb_thread = None
        self._hb_lock = threading.Lock()
        self._keepalive_seq = 0
        self._keepalive_failure_counts = {}
        self._keepalive_retry_forever = bool(getattr(self, "_keepalive_retry_forever", False))
        self._compensation_executor_lock = threading.Lock()
        self._compensation_executor: Optional[ThreadPoolExecutor] = None
        self._compensation_future: Optional[Future] = None
        self._active_replica_lock = threading.RLock()
        self._active_replica_ids = set(self.replicas.keys())
        self._terminal_replica_ids = set()
        self._retry_probe_replica_ids = set()
        self._retry_probe_entered_at: Dict[str, float] = {}
        self._zero_alive_started_at: Dict[str, float] = {}
        self._zero_alive_last_failure_at: Dict[str, float] = {}
        self._disconnect_last_failure_at: Dict[str, float] = {}
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
        configured = _coerce_positive_int(getattr(self, "_heartbeat_worker_count", 0), 0)
        if configured <= 0:
            configured = _coerce_positive_int(str(os.getenv("PYCLOUD_HEARTBEAT_MAX_WORKERS", "") or "").strip(), 0)
        cap = HEARTBEAT_DEFAULT_MAX_WORKERS
        cap = _coerce_positive_int(str(os.getenv("PYCLOUD_HEARTBEAT_MAX_WORKER_CAP", "") or "").strip(), cap)
        if configured > 0:
            return max(1, configured)
        return max(2, min(cap, replica_count))

    def _stale_heartbeat_pending_limit(self) -> int:
        return max(2, min(64, max(1, len(self.replicas)) * STALE_HEARTBEAT_PENDING_PER_REPLICA))

    def _heartbeat_rpc_timeout_sec(self, replica: ExecutionReplicaHandle) -> float:
        explicit = _coerce_positive_float(getattr(replica, "heartbeat_rpc_timeout_sec", 0.0), 0.0)
        if explicit <= 0.0:
            explicit = _coerce_positive_float(str(os.getenv("PYCLOUD_HEARTBEAT_RPC_TIMEOUT_SEC", "") or "").strip(), 0.0)
        if explicit <= 0.0:
            heartbeat_timeout = _coerce_positive_float(getattr(replica, "heartbeat_timeout_sec", 0), 0.0)
            explicit = min(HEARTBEAT_DEFAULT_RPC_TIMEOUT_SEC, heartbeat_timeout) if heartbeat_timeout > 0 else HEARTBEAT_DEFAULT_RPC_TIMEOUT_SEC
        return max(HEARTBEAT_MIN_RPC_TIMEOUT_SEC, explicit)

    def _heartbeat_pending_queue_timeout_sec(self, interval_sec: float) -> float:
        explicit = _coerce_positive_float(getattr(self, "_heartbeat_queue_timeout_override_sec", 0.0), 0.0)
        if explicit <= 0.0:
            explicit = _coerce_positive_float(str(os.getenv("PYCLOUD_HEARTBEAT_QUEUE_TIMEOUT_SEC", "") or "").strip(), 0.0)
        if explicit <= 0.0:
            explicit = max(HEARTBEAT_MIN_QUEUE_TIMEOUT_SEC, max(0.05, float(interval_sec)) * 2.0)
        return max(HEARTBEAT_MIN_QUEUE_TIMEOUT_SEC, explicit)

    def _replica_remote_diagnostics_enabled(self) -> bool:
        value = str(os.getenv("PYCLOUD_HEARTBEAT_REMOTE_DIAGNOSTICS", "") or "").strip().lower()
        if value in {"1", "true", "yes", "on", "debug"}:
            return True
        return bool(getattr(self, "_heartbeat_remote_diagnostics", False))

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
        if self._replica_remote_diagnostics_enabled():
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

    def _timed_heartbeat_replica(
        self,
        node_id: str,
        replica: ExecutionReplicaHandle,
        *,
        seq: int,
        pending_state: Optional[_HeartbeatPending] = None,
    ) -> Any:
        started_at = time.monotonic()
        if pending_state is not None:
            pending_state.started_at = started_at
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

    def _replica_resource_key(self, node_id: str, replica: ExecutionReplicaHandle) -> Tuple[str, str, str]:
        kind = str(getattr(replica, "kind", "") or self.kind or "").strip()
        resource_id = str(getattr(replica, "service_id", "") or getattr(replica, "pool_id", "") or "").strip()
        return kind, resource_id, str(node_id or "").strip()

    def _is_current_replica(self, node_id: str, replica: ExecutionReplicaHandle) -> bool:
        normalized = str(node_id or "").strip()
        if not normalized:
            return False
        current = self.replicas.get(normalized)
        if current is not replica:
            return False
        return self._replica_resource_key(normalized, current) == self._replica_resource_key(normalized, replica)

    def _can_accept_heartbeat_success(self, node_id: str, replica: ExecutionReplicaHandle, *, allow_new: bool = False) -> bool:
        if not self._is_current_replica(node_id, replica):
            return False
        if allow_new:
            return True
        normalized = str(node_id or "").strip()
        return normalized in self._active_replica_snapshot() or normalized in self._retry_probe_replica_snapshot()

    def _mark_replica_heartbeat_success(
        self,
        node_id: str,
        replica: ExecutionReplicaHandle,
        *,
        allow_new: bool = False,
    ) -> None:
        if not self._can_accept_heartbeat_success(node_id, replica, allow_new=allow_new):
            logger.warning(
                "%s keepalive heartbeat success ignored for untrusted replica context=%s",
                self.kind or "execution",
                self._replica_log_context(str(node_id or ""), replica),
            )
            return
        if self._handle_zero_alive_heartbeat_success(node_id, replica):
            return
        normalized_node_id = str(node_id or "").strip()
        was_terminal = self._is_terminal_replica(normalized_node_id)
        self._handle_replica_reported_method_failures(node_id, replica)
        blacklist = getattr(self, "_owner_node_blacklist", None)
        if blacklist is None:
            blacklist = getattr(self, "_create_failure_node_blacklist", {}) or {}
        if normalized_node_id in blacklist:
            return
        if self._is_terminal_replica(normalized_node_id) and not (allow_new and was_terminal):
            return
        self._keepalive_failure_counts.pop(node_id, None)
        self._discard_terminal_replica(node_id)
        self._discard_retry_probe_replica(node_id)
        if hasattr(replica, "failed"):
            replica.failed = False
        if hasattr(replica, "last_error"):
            replica.last_error = ""
        self.failures.pop(node_id, None)
        self._add_active_replica(node_id)

    def _handle_replica_reported_method_failures(self, node_id: str, replica: ExecutionReplicaHandle) -> None:
        hook = getattr(self, "_on_replica_method_failures_reported", None)
        if callable(hook):
            with contextlib.suppress(Exception):
                hook(str(node_id or "").strip(), replica, dict(getattr(replica, "method_failures", {}) or {}))

    def _replica_deployed_ready_for_zero_alive(self, replica: ExecutionReplicaHandle) -> bool:
        readiness = str(getattr(replica, "readiness", "") or "").strip().lower()
        if readiness and readiness != "ready":
            return False
        status = getattr(replica, "status", None)
        kind = str(getattr(replica, "kind", "") or "").strip()
        if status is not None:
            if kind == "service":
                return int(status or 0) == int(pb2.SERVICE_STATUS_RUNNING)
            if kind == "task_pool":
                return str(status or "").strip().upper() == "RUNNING"
        return _coerce_positive_int(getattr(replica, "worker_count", 0), default=0) > 0

    def _mark_zero_alive_node_blacklisted(self, node_id: str, reason: str) -> None:
        hook = getattr(self, "_mark_replica_node_instance_blacklisted", None)
        if callable(hook):
            with contextlib.suppress(Exception):
                hook(str(node_id or "").strip(), str(reason or "").strip())

    def _mark_disconnect_node_blacklisted(self, node_id: str, reason: str) -> None:
        hook = getattr(self, "_mark_replica_node_instance_blacklisted", None)
        if callable(hook):
            with contextlib.suppress(Exception):
                hook(str(node_id or "").strip(), str(reason or "").strip())

    def _record_replica_disconnect_failure(self, node_id: str, replica: ExecutionReplicaHandle, exc: Exception) -> bool:
        normalized = str(node_id or "").strip()
        if not normalized or not self._is_current_replica(normalized, replica):
            return False
        now = time.monotonic()
        last_failure = float(getattr(self, "_disconnect_last_failure_at", {}).get(normalized, 0.0) or 0.0)
        getattr(self, "_disconnect_last_failure_at", {})[normalized] = now
        if last_failure <= 0.0 or now - last_failure > OWNER_ZERO_ALIVE_BLACKLIST_WINDOW_SEC:
            return False
        reason = (
            "repeat owner-node disconnect within "
            f"{OWNER_ZERO_ALIVE_BLACKLIST_WINDOW_SEC:.0f}s: {repr(exc)}"
        )
        self._mark_disconnect_node_blacklisted(normalized, reason)
        self.failures[normalized] = reason
        if hasattr(replica, "failed"):
            replica.failed = True
        if hasattr(replica, "last_error"):
            replica.last_error = reason
        self._discard_active_replica(normalized)
        self._discard_retry_probe_replica(normalized)
        self._mark_terminal_replica(normalized)
        logger.warning(
            "%s keepalive replica disconnected repeatedly context=%s reason=%s",
            self.kind or "execution",
            self._replica_log_context(normalized, replica),
            reason,
        )
        self._wake_keepalive()
        return True

    def _handle_zero_alive_heartbeat_success(self, node_id: str, replica: ExecutionReplicaHandle) -> bool:
        normalized = str(node_id or "").strip()
        if not normalized:
            return False
        alive_workers = max(0, int(getattr(replica, "alive_workers", 0) or 0))
        if alive_workers > 0:
            getattr(self, "_zero_alive_started_at", {}).pop(normalized, None)
            return False
        if not self._replica_deployed_ready_for_zero_alive(replica):
            getattr(self, "_zero_alive_started_at", {}).pop(normalized, None)
            return False
        now = time.monotonic()
        started = float(getattr(self, "_zero_alive_started_at", {}).get(normalized, 0.0) or 0.0)
        if started <= 0.0:
            getattr(self, "_zero_alive_started_at", {})[normalized] = now
            return False
        elapsed = max(0.0, now - started)
        if elapsed < OWNER_ZERO_ALIVE_DISCONNECT_SEC:
            return False
        last_failure = float(getattr(self, "_zero_alive_last_failure_at", {}).get(normalized, 0.0) or 0.0)
        reason = f"alive_workers=0 for {elapsed:.3f}s; treating replica as disconnected"
        getattr(self, "_zero_alive_last_failure_at", {})[normalized] = now
        blacklisted = False
        if last_failure > 0.0 and now - last_failure <= OWNER_ZERO_ALIVE_BLACKLIST_WINDOW_SEC:
            blacklisted = True
            blacklist_reason = (
                f"repeat zero-alive disconnect within {OWNER_ZERO_ALIVE_BLACKLIST_WINDOW_SEC:.0f}s"
            )
            self._mark_zero_alive_node_blacklisted(normalized, blacklist_reason)
            reason = f"{reason}; node_instance blacklisted: {blacklist_reason}"
        self.failures[normalized] = reason
        if hasattr(replica, "failed"):
            replica.failed = True
        if hasattr(replica, "last_error"):
            replica.last_error = reason
        self._keepalive_failure_counts[normalized] = self._heartbeat_failure_threshold(normalized, replica)
        self._discard_active_replica(normalized)
        self._discard_retry_probe_replica(normalized)
        self._mark_terminal_replica(normalized)
        getattr(self, "_zero_alive_started_at", {}).pop(normalized, None)
        logger.warning(
            "%s keepalive replica zero-alive disconnected context=%s elapsed_sec=%.3f blacklisted=%s reason=%s",
            self.kind or "execution",
            self._replica_log_context(normalized, replica),
            elapsed,
            blacklisted,
            reason,
        )
        self._wake_keepalive()
        return True

    def _mark_replica_heartbeat_failure(self, node_id: str, replica: ExecutionReplicaHandle, exc: Exception) -> None:
        if not self._is_current_replica(node_id, replica):
            return
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
        getattr(self, "_zero_alive_started_at", {}).pop(str(node_id or "").strip(), None)
        self._discard_active_replica(node_id)

    def _mark_replica_heartbeat_probe_failure(self, node_id: str, replica: ExecutionReplicaHandle, exc: Exception) -> None:
        if not self._is_current_replica(node_id, replica):
            return
        message = repr(exc)
        self.failures[node_id] = message
        if hasattr(replica, "failed"):
            replica.failed = True
        if hasattr(replica, "last_error"):
            replica.last_error = message
        getattr(self, "_zero_alive_started_at", {}).pop(str(node_id or "").strip(), None)
        self._discard_active_replica(node_id)
        self._mark_retry_probe_replica(node_id)

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

    def _classify_heartbeat_error(self, node_id: str, replica: ExecutionReplicaHandle, exc: Exception) -> HeartbeatErrorKind:
        if self._is_terminal_heartbeat_error(node_id, replica, exc):
            return HeartbeatErrorKind.TERMINAL
        if isinstance(exc, TimeoutError) or self._is_retry_probe_heartbeat_error(node_id, replica, exc):
            return HeartbeatErrorKind.TRANSIENT
        return HeartbeatErrorKind.UNKNOWN

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
        now = time.monotonic()
        lock = getattr(self, "_active_replica_lock", None)
        if lock is None:
            getattr(self, "_retry_probe_replica_ids", set()).add(node_id)
            getattr(self, "_retry_probe_entered_at", {})[node_id] = now
            return
        with lock:
            getattr(self, "_retry_probe_replica_ids", set()).add(node_id)
            getattr(self, "_retry_probe_entered_at", {})[node_id] = now

    def _discard_retry_probe_replica(self, node_id: str) -> None:
        lock = getattr(self, "_active_replica_lock", None)
        if lock is None:
            getattr(self, "_retry_probe_replica_ids", set()).discard(node_id)
            getattr(self, "_retry_probe_entered_at", {}).pop(node_id, None)
            return
        with lock:
            getattr(self, "_retry_probe_replica_ids", set()).discard(node_id)
            getattr(self, "_retry_probe_entered_at", {}).pop(node_id, None)

    def _retry_probe_replica_snapshot(self) -> set[str]:
        lock = getattr(self, "_active_replica_lock", None)
        if lock is None:
            return {str(node_id) for node_id in list(getattr(self, "_retry_probe_replica_ids", set()) or []) if str(node_id)}
        with lock:
            return {str(node_id) for node_id in list(getattr(self, "_retry_probe_replica_ids", set()) or []) if str(node_id)}

    def _retry_probe_ttl_sec(self, node_id: str) -> float:
        replica = self.replicas.get(str(node_id or ""))
        heartbeat_timeout = float(getattr(replica, "heartbeat_timeout_sec", 0) or 0)
        if heartbeat_timeout <= 0.0:
            heartbeat_timeout = 30.0
        return max(1.0, min(60.0, heartbeat_timeout))

    def _expire_retry_probe_replicas(self, *, now: Optional[float] = None) -> set[str]:
        checked_at = time.monotonic() if now is None else float(now)
        expired: set[str] = set()
        lock = getattr(self, "_active_replica_lock", None)
        if lock is None:
            items = list(getattr(self, "_retry_probe_replica_ids", set()) or [])
        else:
            with lock:
                items = list(getattr(self, "_retry_probe_replica_ids", set()) or [])
        for node_id in items:
            normalized = str(node_id or "")
            if not normalized:
                continue
            entered_at = float(getattr(self, "_retry_probe_entered_at", {}).get(normalized, checked_at) or checked_at)
            age_sec = max(0.0, checked_at - entered_at)
            ttl_sec = self._retry_probe_ttl_sec(normalized)
            if age_sec < ttl_sec:
                continue
            expired.add(normalized)
            logger.warning(
                "%s retry_probe expired node_instance_id=%s age_sec=%.3f ttl_sec=%.3f",
                self.kind or "execution",
                normalized,
                age_sec,
                ttl_sec,
            )
            self._discard_retry_probe_replica(normalized)
        return expired

    def _compensation_deferred_by_retry_probe(
        self,
        *,
        resource_name: str = "",
        active: Optional[set[str]] = None,
        desired: int = 0,
        current_node_instance_ids: Optional[set[str]] = None,
        candidate_node_instance_ids: Optional[set[str]] = None,
    ) -> bool:
        expired_retry_probe = self._expire_retry_probe_replicas()
        retry_probe = self._retry_probe_replica_snapshot()
        if not retry_probe:
            return False
        active_snapshot = {str(node_id) for node_id in list(active or set()) if str(node_id)}
        desired_count = int(desired or 0)
        current_snapshot = (
            {str(node_id) for node_id in list(current_node_instance_ids or set()) if str(node_id)}
            if current_node_instance_ids is not None
            else set()
        )
        candidate_snapshot = (
            {str(node_id) for node_id in list(candidate_node_instance_ids or set()) if str(node_id)}
            if candidate_node_instance_ids is not None
            else set()
        )
        stale_retry_probe: set[str] = set()
        effective_retry_probe = set(retry_probe)
        if current_node_instance_ids is not None:
            stale_retry_probe = retry_probe - current_snapshot
            effective_retry_probe = retry_probe & current_snapshot
            for node_id in sorted(stale_retry_probe):
                self._discard_retry_probe_replica(node_id)
        if candidate_node_instance_ids is not None:
            candidate_retry_probe = effective_retry_probe & candidate_snapshot
            candidate_available = candidate_snapshot - effective_retry_probe
            if candidate_available:
                logger.warning(
                    "%s compensation allowed because retry probes do not cover all candidates "
                    "resource_name=%s retry_probe=%s retry_probe_stale=%s retry_probe_expired=%s "
                    "current_nodes=%s active=%s desired=%s candidate_nodes=%s",
                    self.kind or "execution",
                    str(resource_name or ""),
                    sorted(effective_retry_probe),
                    sorted(stale_retry_probe),
                    sorted(expired_retry_probe),
                    sorted(current_snapshot),
                    sorted(active_snapshot),
                    desired_count,
                    sorted(candidate_snapshot),
                )
                return False
            effective_retry_probe = candidate_retry_probe
        if not effective_retry_probe:
            logger.warning(
                "%s compensation allowed because retry probes are stale "
                "resource_name=%s retry_probe=%s retry_probe_stale=%s retry_probe_expired=%s "
                "current_nodes=%s active=%s desired=%s candidate_nodes=%s",
                self.kind or "execution",
                str(resource_name or ""),
                sorted(retry_probe),
                sorted(stale_retry_probe),
                sorted(expired_retry_probe),
                sorted(current_snapshot),
                sorted(active_snapshot),
                desired_count,
                sorted(candidate_snapshot),
            )
            return False
        if desired_count > 0 and not active_snapshot:
            logger.warning(
                "%s compensation allowed despite heartbeat retry probes because no active replicas remain "
                "resource_name=%s retry_probe=%s retry_probe_stale=%s retry_probe_expired=%s "
                "current_nodes=%s active=%s desired=%s candidate_nodes=%s",
                self.kind or "execution",
                str(resource_name or ""),
                sorted(effective_retry_probe),
                sorted(stale_retry_probe),
                sorted(expired_retry_probe),
                sorted(current_snapshot),
                sorted(active_snapshot),
                desired_count,
                sorted(candidate_snapshot),
            )
            return False
        logger.warning(
            "%s compensation deferred while heartbeat retry probes are pending "
            "resource_name=%s retry_probe=%s retry_probe_stale=%s retry_probe_expired=%s "
            "current_nodes=%s active=%s desired=%s candidate_nodes=%s",
            self.kind or "execution",
            str(resource_name or ""),
            sorted(effective_retry_probe),
            sorted(stale_retry_probe),
            sorted(expired_retry_probe),
            sorted(current_snapshot),
            sorted(active_snapshot),
            desired_count,
            sorted(candidate_snapshot),
        )
        return True

    def _run_compensation_attempt(self, *, resource_name: str = "") -> int:
        started_at = time.monotonic()
        added = 0
        try:
            compensate = getattr(self, "try_compensate_replicas", None)
            if not callable(compensate):
                return 0
            added = int(compensate() or 0)
            return added
        except Exception:
            logger.exception(
                "%s compensation failed resource_name=%s",
                self.kind or "execution",
                str(resource_name or ""),
            )
            return 0
        finally:
            elapsed_sec = time.monotonic() - started_at
            if elapsed_sec >= SLOW_COMPENSATION_LOG_SEC:
                logger.warning(
                    "%s compensation slow resource_name=%s elapsed_sec=%.3f added=%s",
                    self.kind or "execution",
                    str(resource_name or ""),
                    elapsed_sec,
                    added,
                )

    def _submit_compensation_attempt(self, *, resource_name: str = "") -> bool:
        if self._is_execution_closed() or self._hb_stop.is_set():
            return False
        with self._compensation_executor_lock:
            future = self._compensation_future
            if future is not None:
                if not future.done():
                    return False
                with contextlib.suppress(Exception):
                    future.result()
                self._compensation_future = None
            executor = self._compensation_executor
            if executor is None:
                executor = ThreadPoolExecutor(
                    max_workers=1,
                    thread_name_prefix=f"{self.kind or 'execution'}-comp",
                )
                self._compensation_executor = executor
            self._compensation_future = executor.submit(
                self._run_compensation_attempt,
                resource_name=resource_name,
            )
            return True

    def _maybe_submit_compensation_after_tick(self, spec: Optional[Dict[str, Any]], *, resource_name: str = "") -> bool:
        if not spec:
            return False
        desired = max(0, int(dict(spec).get("node_count", 0) or 0))
        active_count = len(self._active_replica_snapshot())
        if desired <= 0 or active_count >= desired:
            return False
        interval_sec = 1.0 if active_count <= 0 else 60.0
        now = time.monotonic()
        if now - float(getattr(self, "_last_compensation_attempt_at", 0.0) or 0.0) < interval_sec:
            return False
        setattr(self, "_last_compensation_attempt_at", now)
        return self._submit_compensation_attempt(resource_name=resource_name)

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
        if not self._can_accept_heartbeat_success(node_id, replica):
            return
        count = int(self._keepalive_failure_counts.get(node_id, 0) or 0) + 1
        self._keepalive_failure_counts[node_id] = count
        error_kind = self._classify_heartbeat_error(node_id, replica, exc)
        if count >= self._heartbeat_failure_threshold(node_id, replica):
            if error_kind == HeartbeatErrorKind.TRANSIENT:
                if self._record_replica_disconnect_failure(node_id, replica, exc):
                    return
                self._mark_replica_heartbeat_probe_failure(node_id, replica, exc)
            else:
                self._mark_replica_heartbeat_failure(node_id, replica, exc)
        if error_kind == HeartbeatErrorKind.TRANSIENT:
            self._mark_retry_probe_replica(node_id)

    def _record_terminal_heartbeat_failure(self, node_id: str, replica: ExecutionReplicaHandle, exc: Exception) -> None:
        if not self._can_accept_heartbeat_success(node_id, replica):
            return
        self._keepalive_failure_counts[node_id] = self._heartbeat_failure_threshold(node_id, replica)
        self._mark_replica_heartbeat_failure(node_id, replica, exc)
        self._mark_terminal_replica(node_id)
        hook = getattr(self, "_on_terminal_replica_failure", None)
        if callable(hook):
            with contextlib.suppress(Exception):
                hook(node_id, replica, exc)

    def _heartbeat_new_replica_before_activate(
        self,
        node_id: str,
        replica: ExecutionReplicaHandle,
        *,
        activate: bool = True,
    ) -> bool:
        try:
            self._keepalive_seq += 1
            self._timed_heartbeat_replica(node_id, replica, seq=self._keepalive_seq)
            if activate:
                self._mark_replica_heartbeat_success(node_id, replica, allow_new=True)
            return True
        except Exception as exc:
            logger.warning(
                "%s compensation initial heartbeat failed node_instance_id=%s context=%s error=%r",
                self.kind or "execution",
                str(node_id or ""),
                self._replica_log_context(str(node_id or ""), replica),
                exc,
            )
            self._record_heartbeat_failure(str(node_id or ""), replica, exc)
            return False

    def _wake_keepalive(self) -> None:
        wakeup = getattr(self, "_hb_wakeup", None)
        if wakeup is not None:
            wakeup.set()

    def _handle_heartbeat_exception(self, node_id: str, replica: ExecutionReplicaHandle, exc: Exception) -> None:
        error_kind = self._classify_heartbeat_error(node_id, replica, exc)
        if error_kind == HeartbeatErrorKind.TERMINAL:
            if not self._is_terminal_replica(node_id):
                logger.warning(
                    "%s keepalive replica stopped heartbeat_error_kind=%s context=%s error=%r",
                    self.kind or "execution",
                    error_kind.value,
                    self._replica_log_context(node_id, replica),
                    exc,
                )
            self._record_terminal_heartbeat_failure(node_id, replica, exc)
            return
        logger.warning(
            "%s keepalive heartbeat failed heartbeat_error_kind=%s context=%s error=%r",
            self.kind or "execution",
            error_kind.value,
            self._replica_log_context(node_id, replica),
            exc,
        )
        self._record_heartbeat_failure(node_id, replica, exc)

    def _keepalive_loop(self, interval_sec: float) -> None:
        next_tick = time.monotonic() + max(0.1, float(interval_sec))
        pending: Dict[str, _HeartbeatPending] = {}
        stale_pending: list[_HeartbeatPending] = []
        max_workers = self._heartbeat_max_workers()
        executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix=f"{self.kind or 'execution'}-hb")
        exit_reason = "stop requested"
        try:
            while not self._hb_stop.is_set():
                tick_started_at = time.monotonic()
                completed_count = 0
                failed_count = 0
                submitted_count = 0
                now = time.monotonic()
                active_stale: list[_HeartbeatPending] = []
                for item in stale_pending:
                    if item.future is None:
                        continue
                    if item.future.done():
                        try:
                            item.future.result()
                        except Exception:
                            pass
                    else:
                        active_stale.append(item)
                stale_pending = active_stale
                for node_id, pending_state in list(pending.items()):
                    future = pending_state.future
                    if future is None:
                        pending.pop(node_id, None)
                        continue
                    replica = pending_state.replica
                    if self.replicas.get(node_id) is not replica:
                        future.cancel()
                        pending.pop(node_id, None)
                        continue
                    if not future.done():
                        queued_wait_sec = max(0.0, now - pending_state.submitted_at) if pending_state.started_at <= 0 else 0.0
                        rpc_running_sec = max(0.0, now - pending_state.started_at) if pending_state.started_at > 0 else 0.0
                        rpc_timeout_sec = self._heartbeat_rpc_timeout_sec(replica)
                        queue_timeout_sec = self._heartbeat_pending_queue_timeout_sec(interval_sec)
                        pending_timeout_sec = rpc_timeout_sec if pending_state.started_at > 0 else queue_timeout_sec
                        elapsed_sec = rpc_running_sec if pending_state.started_at > 0 else queued_wait_sec
                        if (
                            elapsed_sec >= pending_timeout_sec
                            and now - pending_state.last_pending_report_at >= pending_timeout_sec
                        ):
                            started_text = "yes" if pending_state.started_at > 0 else "no"
                            pending_exc = TimeoutError(
                                f"heartbeat pending queued_wait_sec={queued_wait_sec:.3f} rpc_running_sec={rpc_running_sec:.3f}"
                            )
                            error_kind = self._classify_heartbeat_error(node_id, replica, pending_exc)
                            logger.warning(
                                "%s keepalive heartbeat pending node_instance_id=%s queued_wait_sec=%.3f "
                                "rpc_running_sec=%.3f queue_timeout_sec=%.3f rpc_timeout_sec=%.3f "
                                "started=%s stale_pending=%s heartbeat_error_kind=%s",
                                self.kind or "execution",
                                node_id,
                                queued_wait_sec,
                                rpc_running_sec,
                                queue_timeout_sec,
                                rpc_timeout_sec,
                                started_text,
                                len(stale_pending),
                                error_kind.value,
                            )
                            self._record_heartbeat_failure(
                                node_id,
                                replica,
                                pending_exc,
                            )
                            failed_count += 1
                            if future.cancel():
                                pending.pop(node_id, None)
                            else:
                                pending.pop(node_id, None)
                                pending_state.last_pending_report_at = now
                                stale_pending.append(pending_state)
                                stale_limit = self._stale_heartbeat_pending_limit()
                                if len(stale_pending) > stale_limit:
                                    dropped = len(stale_pending) - stale_limit
                                    stale_pending = stale_pending[-stale_limit:]
                                    logger.warning(
                                        "%s keepalive stale heartbeat pending limit reached dropped=%s kept=%s heartbeat_error_kind=%s",
                                        self.kind or "execution",
                                        dropped,
                                        stale_limit,
                                        HeartbeatErrorKind.TRANSIENT.value,
                                    )
                        continue
                    pending.pop(node_id, None)
                    try:
                        future.result()
                        completed_count += 1
                        self._mark_replica_heartbeat_success(node_id, replica)
                    except Exception as exc:
                        failed_count += 1
                        self._handle_heartbeat_exception(node_id, replica, exc)

                wait_sec = max(0.0, next_tick - time.monotonic())
                if wait_sec > 0.0:
                    self._hb_wakeup.wait(wait_sec)
                    self._hb_wakeup.clear()
                    if self._hb_stop.is_set():
                        exit_reason = "stop requested"
                        break
                elif self._hb_stop.is_set():
                    exit_reason = "stop requested"
                    break
                for node_id, pending_state in list(pending.items()):
                    future = pending_state.future
                    if future is None:
                        pending.pop(node_id, None)
                        continue
                    replica = pending_state.replica
                    if self.replicas.get(node_id) is not replica:
                        future.cancel()
                        pending.pop(node_id, None)
                        continue
                    if not future.done():
                        continue
                    pending.pop(node_id, None)
                    try:
                        future.result()
                        completed_count += 1
                        self._mark_replica_heartbeat_success(node_id, replica)
                    except Exception as exc:
                        failed_count += 1
                        self._handle_heartbeat_exception(node_id, replica, exc)
                next_tick = time.monotonic() + max(0.1, float(interval_sec))
                self._keepalive_seq += 1
                replicas = self.replicas
                heartbeat_ids = list(self._active_replica_snapshot())
                if bool(getattr(self, "_keepalive_retry_forever", False)):
                    heartbeat_ids = list(dict.fromkeys([*heartbeat_ids, *list(replicas.keys())]))
                else:
                    heartbeat_ids = list(dict.fromkeys([*heartbeat_ids, *list(self._retry_probe_replica_snapshot())]))
                heartbeat_ids = [node_id for node_id in heartbeat_ids if not self._is_terminal_replica(node_id)]
                submitted_count += self._submit_heartbeat_batch(
                    executor=executor,
                    pending=pending,
                    stale_pending=stale_pending,
                    replicas=replicas,
                    heartbeat_ids=heartbeat_ids,
                    max_workers=max_workers,
                    seq=self._keepalive_seq,
                )
                if not self._active_replica_snapshot():
                    can_compensate = bool(getattr(self, "_compensation_spec", None))
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
                    if not (can_retry or can_compensate):
                        exit_reason = "no active replicas and no retry or compensation available"
                        self.failed = True
                        self._hb_stop.set()
                        break
                hook = getattr(self, "_after_keepalive_tick", None)
                if callable(hook):
                    with contextlib.suppress(Exception):
                        hook()
                queued_not_started = sum(
                    1
                    for state in list(pending.values()) + list(stale_pending)
                    if state.future is not None and not state.future.done() and state.started_at <= 0
                )
                tick_elapsed_sec = time.monotonic() - tick_started_at
                logger.info(
                    "%s keepalive tick metrics node_count=%d submitted=%d pending=%d "
                    "queued_not_started=%d completed=%d failed=%d tick_elapsed_sec=%.3f",
                    self.kind or "execution",
                    len(replicas),
                    submitted_count,
                    len(pending) + len(stale_pending),
                    queued_not_started,
                    completed_count,
                    failed_count,
                    tick_elapsed_sec,
                )
            for pending_state in list(pending.values()) + list(stale_pending):
                if pending_state.future is not None:
                    pending_state.future.cancel()
        finally:
            try:
                active = sorted(self._active_replica_snapshot())
                retry_probe = sorted(self._retry_probe_replica_snapshot())
                terminal = sorted(getattr(self, "_terminal_replica_ids", set()) or [])
                replicas = sorted(str(node_id) for node_id in self.replicas.keys() if str(node_id))
                failures = dict(getattr(self, "failures", {}) or {})
                has_compensation = bool(getattr(self, "_compensation_spec", None))
                logger.warning(
                    "%s keepalive loop exited reason=%s replicas=%s active=%s retry_probe=%s "
                    "terminal=%s failures=%s has_compensation=%s pending=%d stale_pending=%d",
                    self.kind or "execution",
                    exit_reason,
                    replicas,
                    active,
                    retry_probe,
                    terminal,
                    failures,
                    has_compensation,
                    len(pending),
                    len(stale_pending),
                )
            except Exception:
                logger.exception("%s keepalive loop exit diagnostics failed", self.kind or "execution")
            executor.shutdown(wait=False, cancel_futures=True)

    def _submit_heartbeat_batch(
        self,
        *,
        executor: ThreadPoolExecutor,
        pending: Dict[str, _HeartbeatPending],
        stale_pending: list[_HeartbeatPending],
        replicas: Dict[str, ExecutionReplicaHandle],
        heartbeat_ids: list[str],
        max_workers: int,
        seq: int,
    ) -> int:
        active_future_count = sum(
            1
            for state in list(pending.values()) + list(stale_pending)
            if state.future is not None and not state.future.done()
        )
        available_submit_slots = max(0, max_workers - active_future_count)
        submitted_count = 0
        for node_id in heartbeat_ids:
            if available_submit_slots <= 0:
                break
            if node_id in pending:
                continue
            replica = replicas.get(node_id)
            if replica is None:
                self._discard_active_replica(node_id)
                continue
            submitted_at = time.monotonic()
            pending_state = _HeartbeatPending(
                replica=replica,
                submitted_at=submitted_at,
                last_pending_report_at=submitted_at,
            )
            future = executor.submit(
                self._timed_heartbeat_replica,
                node_id,
                replica,
                seq=seq,
                pending_state=pending_state,
            )
            pending_state.future = future
            pending[node_id] = pending_state
            submitted_count += 1
            available_submit_slots -= 1
        return submitted_count

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
                self._retry_probe_entered_at = {}
                self._zero_alive_started_at = {}
            if hasattr(self, "_active_nodes"):
                self._active_nodes = self._active_replica_ids
            for replica in self.replicas.values():
                if hasattr(replica, "failed"):
                    replica.failed = False
                if hasattr(replica, "last_error"):
                    replica.last_error = ""
            self._hb_stop.clear()
            self._hb_wakeup.clear()
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
        with self._compensation_executor_lock:
            future = self._compensation_future
            if future is not None and not future.done():
                future.cancel()
            self._compensation_future = None
            executor = self._compensation_executor
            self._compensation_executor = None
        if executor is not None:
            executor.shutdown(wait=False, cancel_futures=True)

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
