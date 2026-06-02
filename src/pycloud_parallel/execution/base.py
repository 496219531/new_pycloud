from __future__ import annotations

"""Shared execution session base classes for the authoritative V1 execution layer."""

import contextlib
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
import logging
import threading
import time
from typing import Any, Dict, Optional, Tuple, Union

from pycloud_parallel.controlplane.infocenter_client import InfoCenterNode
from pycloud_parallel.controlplane.session_handle import ExecutionReplicaHandle
from pycloud_parallel.controlplane.session_model import (
    ExecutionReplicaSnapshot,
    ExecutionSessionStatus,
    SessionLease,
    build_execution_session_status,
)
from pycloud_parallel.proto.v1 import pycloud_v1_pb2 as pb2


logger = logging.getLogger(__name__)
SLOW_HEARTBEAT_LOG_SEC = 2.0


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
        self._active_replica_ids = set(self.replicas.keys())
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

    def _default_keepalive_interval_sec(self, interval_sec: Optional[float] = None) -> float:
        if interval_sec is not None:
            return max(0.5, float(interval_sec))
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
        if hasattr(replica, "failed"):
            replica.failed = False
        if hasattr(replica, "last_error"):
            replica.last_error = ""
        self.failures.pop(node_id, None)
        self._active_replica_ids.add(node_id)

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
        self._active_replica_ids.discard(node_id)

    def _record_heartbeat_failure(self, node_id: str, replica: ExecutionReplicaHandle, exc: Exception) -> None:
        count = int(self._keepalive_failure_counts.get(node_id, 0) or 0) + 1
        self._keepalive_failure_counts[node_id] = count
        if count >= self._heartbeat_failure_threshold(node_id, replica):
            self._mark_replica_heartbeat_failure(node_id, replica, exc)

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
                                "%s keepalive heartbeat pending node_instance_id=%s elapsed_sec=%.3f",
                                self.kind or "execution",
                                node_id,
                                now - started_at,
                            )
                            self._record_heartbeat_failure(
                                node_id,
                                replica,
                                TimeoutError(f"heartbeat pending for {now - started_at:.3f}s"),
                            )
                            if bool(getattr(self, "_keepalive_retry_forever", False)):
                                pending[node_id] = (future, replica, started_at, now)
                            else:
                                future.cancel()
                                pending.pop(node_id, None)
                        continue
                    pending.pop(node_id, None)
                    try:
                        future.result()
                        self._mark_replica_heartbeat_success(node_id, replica)
                    except Exception as exc:
                        logger.warning(
                            "%s keepalive heartbeat failed node_instance_id=%s error=%r",
                            self.kind or "execution",
                            node_id,
                            exc,
                        )
                        self._record_heartbeat_failure(node_id, replica, exc)

                wait_sec = max(0.0, next_tick - time.monotonic())
                if self._hb_stop.wait(wait_sec):
                    break
                next_tick += max(0.1, float(interval_sec))
                self._keepalive_seq += 1
                replicas = self.replicas
                active_ids = list(self._active_replica_ids)
                if len(active_ids) == 1 and not pending and not bool(getattr(self, "_keepalive_retry_forever", False)):
                    node_id = active_ids[0]
                    replica = replicas.get(node_id)
                    if replica is None:
                        self._active_replica_ids.discard(node_id)
                    else:
                        try:
                            self._timed_heartbeat_replica(node_id, replica, seq=self._keepalive_seq)
                            self._mark_replica_heartbeat_success(node_id, replica)
                        except Exception as exc:
                            logger.warning(
                                "%s keepalive heartbeat failed node_instance_id=%s error=%r",
                                self.kind or "execution",
                                node_id,
                                exc,
                            )
                            self._record_heartbeat_failure(node_id, replica, exc)
                heartbeat_ids = list(self._active_replica_ids)
                if bool(getattr(self, "_keepalive_retry_forever", False)):
                    heartbeat_ids = list(dict.fromkeys([*heartbeat_ids, *list(replicas.keys())]))
                for node_id in heartbeat_ids:
                    if (
                        len(active_ids) == 1
                        and not pending
                        and node_id == active_ids[0]
                        and not bool(getattr(self, "_keepalive_retry_forever", False))
                    ):
                        continue
                    if node_id in pending:
                        continue
                    replica = replicas.get(node_id)
                    if replica is None:
                        self._active_replica_ids.discard(node_id)
                        continue
                    pending[node_id] = (
                        executor.submit(self._timed_heartbeat_replica, node_id, replica, seq=self._keepalive_seq),
                        replica,
                        time.monotonic(),
                        time.monotonic(),
                    )
                if not self._active_replica_ids:
                    if not (bool(getattr(self, "_keepalive_retry_forever", False)) and self.replicas):
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
            self._active_replica_ids = set(self.replicas.keys())
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
