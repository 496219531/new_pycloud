from __future__ import annotations

"""Shared execution session base classes for the authoritative V1 execution layer."""

import contextlib
from dataclasses import dataclass
import threading
import time
from typing import Any, Dict, Optional, Union

from pycloud_parallel.controlplane.infocenter_client import InfoCenterNode
from pycloud_parallel.controlplane.session_handle import ExecutionReplicaHandle
from pycloud_parallel.controlplane.session_model import (
    ExecutionReplicaSnapshot,
    ExecutionSessionStatus,
    SessionLease,
    build_execution_session_status,
)
from pycloud_parallel.proto.v1 import pycloud_v1_pb2 as pb2


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

    def _heartbeat_replica(self, node_id: str, replica: ExecutionReplicaHandle, *, seq: int) -> Any:
        del node_id
        try:
            return replica.heartbeat(seq=seq)
        except TypeError:
            return replica.heartbeat()

    def _mark_replica_heartbeat_success(self, node_id: str, replica: ExecutionReplicaHandle) -> None:
        self._keepalive_failure_counts.pop(node_id, None)
        if hasattr(replica, "failed"):
            replica.failed = False
        if hasattr(replica, "last_error"):
            replica.last_error = ""
        self.failures.pop(node_id, None)

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

    def _keepalive_loop(self, interval_sec: float) -> None:
        next_tick = time.monotonic() + max(0.1, float(interval_sec))
        while not self._hb_stop.is_set():
            now = time.monotonic()
            wait_sec = max(0.0, next_tick - now)
            if self._hb_stop.wait(wait_sec):
                break
            next_tick += max(0.1, float(interval_sec))
            self._keepalive_seq += 1
            replicas = self.replicas
            for node_id in list(self._active_replica_ids):
                replica = replicas.get(node_id)
                if replica is None:
                    self._active_replica_ids.discard(node_id)
                    continue
                try:
                    self._heartbeat_replica(node_id, replica, seq=self._keepalive_seq)
                    self._mark_replica_heartbeat_success(node_id, replica)
                except Exception as exc:
                    count = int(self._keepalive_failure_counts.get(node_id, 0) or 0) + 1
                    self._keepalive_failure_counts[node_id] = count
                    if count >= self._heartbeat_failure_threshold(node_id, replica):
                        self._mark_replica_heartbeat_failure(node_id, replica, exc)
            if not self._active_replica_ids:
                self.failed = True
                self._hb_stop.set()
                break
            hook = getattr(self, "_after_keepalive_tick", None)
            if callable(hook):
                with contextlib.suppress(Exception):
                    hook()

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
