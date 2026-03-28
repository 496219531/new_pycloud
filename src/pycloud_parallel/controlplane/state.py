from __future__ import annotations

"""In-memory state backends for InfoCenter and NodeControl."""

import hashlib
import threading
import uuid
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Deque, Dict, Iterable, List, Optional, Tuple

from google.protobuf import json_format
from google.protobuf import struct_pb2
from google.protobuf import timestamp_pb2

from pycloud_parallel.controlplane.hooks import InMemoryResultHook
from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def dt_to_ts(dt: datetime) -> timestamp_pb2.Timestamp:
    ts = timestamp_pb2.Timestamp()
    ts.FromDatetime(dt)
    return ts


def struct_to_dict(data: struct_pb2.Struct) -> dict:
    return json_format.MessageToDict(data, preserving_proto_field_name=True)


def dict_to_struct(data: Optional[dict]) -> struct_pb2.Struct:
    out = struct_pb2.Struct()
    if data:
        out.update(data)
    return out


@dataclass
class NodeMetricsState:
    queued: int = 0
    inflight: int = 0
    running: int = 0
    credit: int = 0
    cpu_percent: float = 0.0
    mem_percent: float = 0.0


@dataclass
class NodeState:
    node_id: str
    control_addr: str
    capacity: int
    queue_capacity: int
    tags: List[str] = field(default_factory=list)
    version: str = ""
    metadata: Dict[str, str] = field(default_factory=dict)
    healthy: bool = True
    last_seen_at: datetime = field(default_factory=utc_now)
    metrics: NodeMetricsState = field(default_factory=NodeMetricsState)


class InfoCenterState:
    def __init__(self, *, lease_ttl_sec: int = 90, heartbeat_interval_sec: int = 30) -> None:
        self.lease_ttl_sec = max(1, lease_ttl_sec)
        self.heartbeat_interval_sec = max(1, heartbeat_interval_sec)
        self._lock = threading.Lock()
        self._nodes: Dict[str, NodeState] = {}

    def register_node(self, request: pb2.RegisterNodeRequest) -> NodeState:
        now = utc_now()
        with self._lock:
            state = self._nodes.get(request.node_id)
            if state is None:
                state = NodeState(
                    node_id=request.node_id,
                    control_addr=request.control_addr,
                    capacity=max(1, request.capacity),
                    queue_capacity=max(1, request.queue_capacity),
                )
                self._nodes[request.node_id] = state
            state.control_addr = request.control_addr
            state.capacity = max(1, request.capacity)
            state.queue_capacity = max(1, request.queue_capacity)
            state.tags = list(request.tags)
            state.version = request.version
            state.metadata = dict(request.metadata)
            state.healthy = True
            state.last_seen_at = now
            if state.metrics.credit == 0:
                state.metrics.credit = state.queue_capacity
            return state

    def heartbeat(self, request: pb2.HeartbeatNodeRequest) -> Optional[NodeState]:
        now = utc_now()
        with self._lock:
            state = self._nodes.get(request.node_id)
            if state is None:
                return None
            state.healthy = bool(request.healthy)
            state.last_seen_at = now
            state.metrics = NodeMetricsState(
                queued=max(0, request.metrics.queued),
                inflight=max(0, request.metrics.inflight),
                running=max(0, request.metrics.running),
                credit=request.metrics.credit,
                cpu_percent=float(request.metrics.cpu_percent),
                mem_percent=float(request.metrics.mem_percent),
            )
            return state

    def list_nodes(self, *, healthy_only: bool, tags: Iterable[str], limit: int) -> List[NodeState]:
        now = utc_now()
        filter_tags = set(tags)
        with self._lock:
            out: List[NodeState] = []
            for state in self._nodes.values():
                stale = (now - state.last_seen_at).total_seconds() > float(self.lease_ttl_sec)
                is_healthy = state.healthy and not stale
                if healthy_only and not is_healthy:
                    continue
                if filter_tags and not filter_tags.issubset(set(state.tags)):
                    continue
                out.append(
                    NodeState(
                        node_id=state.node_id,
                        control_addr=state.control_addr,
                        capacity=state.capacity,
                        queue_capacity=state.queue_capacity,
                        tags=list(state.tags),
                        version=state.version,
                        metadata=dict(state.metadata),
                        healthy=is_healthy,
                        last_seen_at=state.last_seen_at,
                        metrics=NodeMetricsState(**vars(state.metrics)),
                    )
                )
            out.sort(key=lambda n: (not n.healthy, -(n.metrics.credit)))
            return out[: max(1, limit)]


@dataclass
class CodeArtifact:
    code_version: str
    path: str
    size_bytes: int
    created_at: datetime


@dataclass
class TaskState:
    task_id: str
    client_id: str
    code_version: str
    execution_mode: int
    payload: dict
    timeout_hint_sec: int
    priority: int
    status: int = pb2.TASK_STATUS_QUEUED
    attempt: int = 1
    worker_id: str = ""
    lease_id: str = ""
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    last_heartbeat_at: Optional[datetime] = None
    cancel_requested: bool = False
    result: Optional[dict] = None
    error_type: str = ""
    error_message: str = ""

    def as_result(self) -> pb2.TaskResult:
        item = pb2.TaskResult(
            task_id=self.task_id,
            status=self.status,
            attempt=self.attempt,
            started_at=dt_to_ts(self.started_at or utc_now()),
            finished_at=dt_to_ts(self.finished_at or utc_now()),
            result=dict_to_struct(self.result),
            error=pb2.TaskError(type=self.error_type, message=self.error_message),
        )
        return item


class NodeControlState:
    def __init__(
        self,
        *,
        node_id: str,
        worker_capacity: int = 32,
        queue_capacity: int = 4000,
        heartbeat_timeout_sec: int = 90,
        max_retries: int = 3,
        monitor_interval_sec: int = 10,
        artifact_dir: str = "Local_DB/code_cache",
    ) -> None:
        self.node_id = node_id
        self.worker_capacity = max(1, worker_capacity)
        self.queue_capacity = max(1, queue_capacity)
        self.heartbeat_timeout_sec = max(5, heartbeat_timeout_sec)
        self.max_retries = max(0, max_retries)
        self.monitor_interval_sec = max(1, monitor_interval_sec)
        self.started_at = utc_now()

        self._lock = threading.Lock()
        self._cv = threading.Condition(self._lock)
        self._pending: Deque[str] = deque()
        self._tasks: Dict[str, TaskState] = {}
        self._codes: Dict[str, CodeArtifact] = {}
        self._result_hook = InMemoryResultHook()

        self._artifact_dir = Path(artifact_dir)
        self._artifact_dir.mkdir(parents=True, exist_ok=True)

        self._stop_event = threading.Event()
        self._monitor = threading.Thread(target=self._monitor_loop, name="nodecontrol-monitor", daemon=True)
        self._monitor.start()

    def close(self) -> None:
        self._stop_event.set()
        self._monitor.join(timeout=1.0)

    def put_code(
        self,
        *,
        sha256: str,
        filename: str,
        chunks: Iterable[bytes],
    ) -> Tuple[CodeArtifact, bool]:
        h = hashlib.sha256()
        blob = bytearray()
        for part in chunks:
            if not part:
                continue
            h.update(part)
            blob.extend(part)

        digest = h.hexdigest()
        expected = sha256.replace("sha256:", "").strip().lower()
        if expected and expected != digest:
            raise ValueError(f"sha256 mismatch: expected={expected}, actual={digest}")

        version = f"sha256:{digest}"
        with self._lock:
            existing = self._codes.get(version)
            if existing is not None:
                return existing, True

        suffix = Path(filename).suffix or ".bin"
        path = self._artifact_dir / f"{digest}{suffix}"
        path.write_bytes(bytes(blob))
        artifact = CodeArtifact(
            code_version=version,
            path=str(path),
            size_bytes=len(blob),
            created_at=utc_now(),
        )
        with self._lock:
            self._codes[version] = artifact
        return artifact, False

    def has_code_version(self, code_version: str) -> bool:
        with self._lock:
            return code_version in self._codes

    def submit_tasks(self, request: pb2.SubmitTasksRequest) -> Tuple[List[pb2.TaskAccepted], List[pb2.TaskRejected], int]:
        accepted: List[pb2.TaskAccepted] = []
        rejected: List[pb2.TaskRejected] = []
        with self._cv:
            if request.code_version not in self._codes:
                for item in request.tasks:
                    rejected.append(
                        pb2.TaskRejected(
                            task_id=item.task_id,
                            code=pb2.ERROR_CODE_UNKNOWN_CODE_VERSION,
                            message=f"unknown code_version: {request.code_version}",
                        )
                    )
                return accepted, rejected, self.credit_locked()

            for item in request.tasks:
                if item.task_id in self._tasks:
                    rejected.append(
                        pb2.TaskRejected(
                            task_id=item.task_id,
                            code=pb2.ERROR_CODE_DUPLICATE_TASK,
                            message="duplicate task_id",
                        )
                    )
                    continue

                if self.credit_locked() <= 0:
                    rejected.append(
                        pb2.TaskRejected(
                            task_id=item.task_id,
                            code=pb2.ERROR_CODE_NO_CREDIT,
                            message="node queue/inflight is full",
                        )
                    )
                    continue

                record = TaskState(
                    task_id=item.task_id,
                    client_id=request.client_id,
                    code_version=request.code_version,
                    execution_mode=request.execution_mode,
                    payload=struct_to_dict(item.payload),
                    timeout_hint_sec=max(0, item.timeout_hint_sec),
                    priority=max(1, item.priority or 1),
                )
                self._tasks[item.task_id] = record
                self._pending.append(item.task_id)
                accepted.append(pb2.TaskAccepted(task_id=item.task_id, status=pb2.TASK_STATUS_QUEUED))
            if accepted:
                self._cv.notify_all()
            return accepted, rejected, self.credit_locked()

    def poll_task(self, worker_id: str) -> Optional[pb2.TaskEnvelope]:
        with self._cv:
            while self._pending:
                task_id = self._pending.popleft()
                task = self._tasks.get(task_id)
                if task is None:
                    continue
                if task.status != pb2.TASK_STATUS_QUEUED:
                    continue
                if task.cancel_requested:
                    task.status = pb2.TASK_STATUS_CANCELLED
                    task.finished_at = utc_now()
                    self._publish_result_locked(task)
                    continue

                now = utc_now()
                task.status = pb2.TASK_STATUS_RUNNING
                task.worker_id = worker_id
                task.lease_id = str(uuid.uuid4())
                task.started_at = now
                task.last_heartbeat_at = now
                return pb2.TaskEnvelope(
                    task_id=task.task_id,
                    code_version=task.code_version,
                    attempt=task.attempt,
                    execution_mode=task.execution_mode,
                    payload=dict_to_struct(task.payload),
                    lease_id=task.lease_id,
                    lease_ttl_sec=self.heartbeat_timeout_sec,
                )
            return None

    def heartbeat_task(self, request: pb2.HeartbeatTaskRequest) -> Tuple[bool, bool]:
        with self._lock:
            task = self._tasks.get(request.task_id)
            if task is None:
                return False, False
            if task.attempt != request.attempt:
                return False, False
            if task.status not in (pb2.TASK_STATUS_RUNNING, pb2.TASK_STATUS_CANCELLED):
                return False, False
            task.last_heartbeat_at = utc_now()
            return True, task.cancel_requested

    def report_result(self, request: pb2.ReportResultRequest) -> bool:
        with self._cv:
            task = self._tasks.get(request.task_id)
            if task is None:
                return False
            if task.attempt != request.attempt:
                return False
            if task.status not in (pb2.TASK_STATUS_RUNNING, pb2.TASK_STATUS_CANCELLED):
                return False

            task.finished_at = utc_now()
            task.last_heartbeat_at = task.finished_at
            if request.status == pb2.TASK_STATUS_SUCCEEDED:
                task.status = pb2.TASK_STATUS_SUCCEEDED
                task.result = struct_to_dict(request.result)
                task.error_type = ""
                task.error_message = ""
            else:
                task.status = pb2.TASK_STATUS_FAILED_USER
                task.result = None
                task.error_type = request.error.type
                task.error_message = request.error.message

            self._publish_result_locked(task)
            self._cv.notify_all()
            return True

    def pull_results(self, request: pb2.PullResultsRequest) -> Tuple[List[pb2.TaskResult], str]:
        return self._result_hook.pull(
            request.client_id,
            limit=max(1, request.limit or 100),
            wait_ms=max(0, request.wait_ms),
            cursor=request.cursor,
        )

    def cancel_tasks(self, request: pb2.CancelTasksRequest) -> Tuple[List[str], List[str], List[str]]:
        cancelled: List[str] = []
        not_found: List[str] = []
        already_done: List[str] = []
        with self._cv:
            for task_id in request.task_ids:
                task = self._tasks.get(task_id)
                if task is None:
                    not_found.append(task_id)
                    continue

                if task.status in (pb2.TASK_STATUS_SUCCEEDED, pb2.TASK_STATUS_FAILED_USER, pb2.TASK_STATUS_FAILED_INFRA):
                    already_done.append(task_id)
                    continue

                task.cancel_requested = True
                if task.status == pb2.TASK_STATUS_QUEUED:
                    task.status = pb2.TASK_STATUS_CANCELLED
                    task.finished_at = utc_now()
                    task.error_type = "Cancelled"
                    task.error_message = request.reason or "cancelled by client"
                    self._publish_result_locked(task)
                cancelled.append(task_id)
            if cancelled:
                self._cv.notify_all()
        return cancelled, not_found, already_done

    def metrics(self) -> Dict[str, int]:
        with self._lock:
            queued = self._queued_count_locked()
            inflight = self._inflight_count_locked()
            credit = max(0, self.queue_capacity - (queued + inflight))
            return {
                "queued": queued,
                "inflight": inflight,
                "running": inflight,
                "credit": credit,
                "queue_capacity": self.queue_capacity,
                "worker_capacity": self.worker_capacity,
                "uptime_sec": int((utc_now() - self.started_at).total_seconds()),
            }

    def credit_locked(self) -> int:
        return max(0, self.queue_capacity - (self._queued_count_locked() + self._inflight_count_locked()))

    def _queued_count_locked(self) -> int:
        return sum(1 for task in self._tasks.values() if task.status == pb2.TASK_STATUS_QUEUED)

    def _inflight_count_locked(self) -> int:
        return sum(1 for task in self._tasks.values() if task.status == pb2.TASK_STATUS_RUNNING)

    def _publish_result_locked(self, task: TaskState) -> None:
        result = task.as_result()
        self._result_hook.push(task.client_id, result)

    def _monitor_loop(self) -> None:
        while not self._stop_event.wait(self.monitor_interval_sec):
            self._handle_timeouts()

    def _handle_timeouts(self) -> None:
        now = utc_now()
        with self._cv:
            mutated = False
            for task in self._tasks.values():
                if task.status != pb2.TASK_STATUS_RUNNING:
                    continue
                if task.last_heartbeat_at is None:
                    continue
                diff = (now - task.last_heartbeat_at).total_seconds()
                if diff <= self.heartbeat_timeout_sec:
                    continue
                if task.attempt < self.max_retries:
                    task.attempt += 1
                    task.status = pb2.TASK_STATUS_QUEUED
                    task.worker_id = ""
                    task.lease_id = ""
                    task.started_at = None
                    task.last_heartbeat_at = None
                    self._pending.append(task.task_id)
                else:
                    task.status = pb2.TASK_STATUS_FAILED_INFRA
                    task.finished_at = now
                    task.error_type = "InfraTimeout"
                    task.error_message = "heartbeat timeout"
                    self._publish_result_locked(task)
                mutated = True

            if mutated:
                self._cv.notify_all()
