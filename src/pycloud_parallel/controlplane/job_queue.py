from __future__ import annotations

import base64
import os
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
import tempfile
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional, Sequence

from pycloud_parallel.controlplane.client import TaskPoolSession
from pycloud_parallel.controlplane.state import _invoke_user_callable, _load_user_module
from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _artifact_suffix(package_format: str) -> str:
    normalized = str(package_format or "").strip().lower()
    if normalized == "tar.gz":
        return ".tar.gz"
    if normalized == "zip":
        return ".zip"
    if normalized == "whl":
        return ".whl"
    return ".py"


def _task_result_to_dict(item: pb2.TaskResult) -> Dict[str, object]:
    detail: Dict[str, object] = {}
    if item.result:
        detail = {
            "result": {
                key: value
                for key, value in item.result.items()
            }
        }
    elif item.error and (item.error.type or item.error.message):
        detail = {
            "error": {
                "type": str(item.error.type or ""),
                "message": str(item.error.message or ""),
            }
        }
    return {
        "task_id": str(item.task_id or ""),
        "job_id": str(item.job_id or ""),
        "status": int(item.status),
        "status_text": pb2.TaskStatus.Name(item.status),
        "attempt": int(item.attempt or 0),
        **detail,
    }


@dataclass
class JobState:
    job_id: str
    client_id: str
    priority: int
    status: str
    submitted_at: datetime
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    code_version: str = ""
    entry_module: str = ""
    entry_callable: str = "run"
    payload: Dict[str, object] = field(default_factory=dict)
    checkpoint: Dict[str, object] = field(default_factory=dict)
    cancel_requested: bool = False
    error: str = ""
    results: List[Dict[str, object]] = field(default_factory=list)

    def as_dict(self) -> Dict[str, object]:
        return {
            "job_id": self.job_id,
            "client_id": self.client_id,
            "priority": self.priority,
            "status": self.status,
            "submitted_at": self.submitted_at.isoformat(),
            "started_at": self.started_at.isoformat() if self.started_at else "",
            "finished_at": self.finished_at.isoformat() if self.finished_at else "",
            "code_version": self.code_version,
            "entry_module": self.entry_module,
            "entry_callable": self.entry_callable,
            "payload": dict(self.payload),
            "checkpoint": dict(self.checkpoint),
            "cancel_requested": bool(self.cancel_requested),
            "error": self.error,
            "results": list(self.results),
        }


class JobQueueManager:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._cv = threading.Condition(self._lock)
        self._jobs: Dict[str, JobState] = {}
        self._running_job_id = ""
        self._current_executor: Any = None
        self._controlplane_target = ""
        self._stop = False
        self._thread: Optional[threading.Thread] = None
        self._retention_sec = max(60, int(os.getenv("PYCLOUD_JOB_QUEUE_RETENTION_SEC", "3600") or 3600))

    def start(self, *, controlplane_target: str) -> None:
        with self._lock:
            self._controlplane_target = str(controlplane_target or "").strip()
            if self._thread is not None:
                return
            self._thread = threading.Thread(target=self._loop, name="job-queue-scheduler", daemon=True)
            self._thread.start()

    def close(self) -> None:
        with self._cv:
            self._stop = True
            if self._running_job_id:
                state = self._jobs.get(self._running_job_id)
                if state is not None:
                    state.cancel_requested = True
            self._cv.notify_all()
        thread = self._thread
        if thread is not None:
            thread.join(timeout=1.0)
            if thread.is_alive():
                return
        with self._lock:
            executor = self._current_executor
        if executor is not None:
            try:
                executor.close()
            except Exception:
                pass

    def submit_job(self, payload: Dict[str, object]) -> JobState:
        job_id = str(payload.get("job_id", "") or "").strip() or f"jobq-{uuid.uuid4().hex}"
        client_id = str(payload.get("client_id", "") or "").strip() or f"job-client-{uuid.uuid4().hex[:8]}"
        priority = max(0, int(payload.get("priority", 0) or 0))
        state = JobState(
            job_id=job_id,
            client_id=client_id,
            priority=priority,
            status="WAITING",
            submitted_at=utc_now(),
            code_version=str(payload.get("code_version", "") or "").strip(),
            entry_module=str(payload.get("entry_module", "") or "").strip(),
            entry_callable=str(payload.get("entry_callable", "") or "run").strip() or "run",
            payload=dict(payload),
        )
        with self._cv:
            self._jobs[job_id] = state
            self._cv.notify_all()
        return state

    def get_job(self, job_id: str) -> Optional[JobState]:
        with self._lock:
            return self._jobs.get(str(job_id or "").strip())

    def cancel_job(self, job_id: str) -> Optional[JobState]:
        normalized = str(job_id or "").strip()
        with self._cv:
            state = self._jobs.get(normalized)
            if state is None:
                return None
            if state.status == "WAITING":
                state.status = "CANCELLED"
                state.finished_at = utc_now()
                state.cancel_requested = True
                return state
            if state.status == "RUNNING":
                state.cancel_requested = True
                executor = self._current_executor
                if executor is not None and getattr(executor, "job_id", "") == state.job_id:
                    try:
                        executor.cancel_job(reason="job queue cancel", job_id=state.job_id)
                    except Exception:
                        pass
                return state
            return state

    def _pick_next_job_locked(self) -> Optional[JobState]:
        waiting = [job for job in self._jobs.values() if job.status == "WAITING" and not job.cancel_requested]
        if not waiting:
            return None
        waiting.sort(key=lambda item: (-int(item.priority), item.submitted_at.timestamp(), item.job_id))
        return waiting[0]

    def _cleanup_jobs_locked(self) -> None:
        if self._retention_sec <= 0:
            return
        now = utc_now()
        expired = []
        for job_id, state in self._jobs.items():
            if state.status not in ("SUCCEEDED", "FAILED", "CANCELLED"):
                continue
            finished_at = state.finished_at or state.submitted_at
            if (now - finished_at).total_seconds() > self._retention_sec:
                expired.append(job_id)
        for job_id in expired:
            self._jobs.pop(job_id, None)

    def _loop(self) -> None:
        while True:
            with self._cv:
                while not self._stop and self._running_job_id:
                    self._cv.wait(timeout=0.1)
                if self._stop:
                    return
                self._cleanup_jobs_locked()
                next_job = self._pick_next_job_locked()
                if next_job is None:
                    self._cv.wait(timeout=0.1)
                    continue
                next_job.status = "RUNNING"
                next_job.started_at = utc_now()
                self._running_job_id = next_job.job_id
            self._run_job(next_job.job_id)
            with self._cv:
                self._running_job_id = ""
                self._current_executor = None
                self._cv.notify_all()

    def _expand_subtasks(self, payload: Dict[str, object]) -> List[Dict[str, object]]:
        subtasks = payload.get("subtasks") or []
        if isinstance(subtasks, list) and subtasks:
            return [dict(item) for item in subtasks if isinstance(item, dict)]

        driver_blob_b64 = str(payload.get("driver_blob_b64", "") or "").strip()
        if not driver_blob_b64:
            return []

        package_format = str(payload.get("driver_package_format", "py") or "py").strip() or "py"
        entry_module = str(payload.get("driver_entry_module", "") or "").strip()
        entry_callable = str(payload.get("driver_entry_callable", "run") or "run").strip() or "run"
        driver_payload = dict(payload.get("driver_payload") or {})

        fd, tmp_name = tempfile.mkstemp(prefix="pycloud-job-driver-", suffix=_artifact_suffix(package_format))
        os.close(fd)
        tmp_path = Path(tmp_name)
        try:
            tmp_path.write_bytes(base64.b64decode(driver_blob_b64.encode("utf-8")))
            module = _load_user_module(
                str(tmp_path),
                entry_module=entry_module,
                package_format=package_format,
                dependency_path="",
            )
            fn = getattr(module, entry_callable, None)
            if fn is None or not callable(fn):
                raise RuntimeError(f"driver callable not found: {entry_callable}")
            produced = _invoke_user_callable(fn, driver_payload)
            if isinstance(produced, dict) and isinstance(produced.get("subtasks"), list):
                produced = produced["subtasks"]
            if not isinstance(produced, list):
                raise RuntimeError("driver must return list[dict] or {'subtasks': list[dict]}")
            subtasks = [dict(item) for item in produced if isinstance(item, dict)]
            if not subtasks:
                raise RuntimeError("driver returned no valid subtasks")
            return subtasks
        finally:
            tmp_path.unlink(missing_ok=True)

    def _run_job(self, job_id: str) -> None:
        with self._lock:
            state = self._jobs.get(job_id)
            if state is None:
                return
            payload = dict(state.payload)
            client_id = state.client_id
            job_id_snapshot = state.job_id

        try:
            with ThreadPoolExecutor(max_workers=1, thread_name_prefix="jobq-expand") as executor:
                fut = executor.submit(self._expand_subtasks, payload)
                subtasks = fut.result(timeout=float(payload.get("driver_timeout_sec", 120.0) or 120.0))
        except Exception as exc:
            with self._lock:
                state = self._jobs.get(job_id)
                if state is not None:
                    state.status = "FAILED"
                    state.finished_at = utc_now()
                    state.error = f"driver failed: {exc}"
            return
        if not subtasks:
            with self._lock:
                state = self._jobs.get(job_id)
                if state is not None:
                    state.status = "FAILED"
                    state.finished_at = utc_now()
                    state.error = "job payload must provide non-empty subtasks list or valid driver"
            return

        kwargs = {
            "infocenter_target": self._controlplane_target,
            "client_id": client_id,
            "job_id": job_id_snapshot,
            "runtime": str(payload.get("runtime", "py3") or "py3"),
            "entry_module": str(payload.get("entry_module", "") or "").strip(),
            "entry_callable": str(payload.get("entry_callable", "run") or "run").strip() or "run",
            "package_format": str(payload.get("package_format", "") or "").strip(),
            "export_mode": str(payload.get("export_mode", "single") or "single").strip() or "single",
            "export_methods": list(payload.get("export_methods") or ()),
            "dependency_allowlist": list(payload.get("dependency_allowlist") or ()),
            "managed_global_names": list(payload.get("managed_global_names") or ()),
            "healthy_only": bool(payload.get("healthy_only", True)),
            "tags": list(payload.get("tags") or ()),
            "node_ids": list(payload.get("node_ids") or ()),
            "node_count": int(payload.get("node_count", 0) or 0),
            "node_limit": int(payload.get("node_limit", 100) or 100),
            "require_credit": bool(payload.get("require_credit", True)),
            "preferred_runtime_key": str(payload.get("preferred_runtime_key", "") or "").strip(),
            "timeout_sec": float(payload.get("timeout_sec", 10.0) or 10.0),
        }

        code_version = str(payload.get("code_version", "") or "").strip()
        if code_version:
            kwargs["code_version"] = code_version
        else:
            blob_b64 = str(payload.get("blob_b64", "") or "").strip()
            if not blob_b64:
                with self._lock:
                    state = self._jobs.get(job_id)
                    if state is not None:
                        state.status = "FAILED"
                        state.finished_at = utc_now()
                        state.error = "job must provide either code_version or blob_b64"
                return
            kwargs["blob"] = base64.b64decode(blob_b64.encode("utf-8"))
        artifact_path = str(payload.get("artifact_path", "") or "").strip()

        executor: Optional[Any] = None
        try:
            executor = TaskPoolSession.from_infocenter(
                    infocenter_target=self._controlplane_target,
                    job_id=job_id_snapshot,
                    owner_client_id=kwargs.get("client_id") or client_id,
                    pool_name=str(payload.get("pool_name", "") or f"job-pool-{job_id_snapshot}"),
                    blob=kwargs.get("blob"),
                    artifact_path=artifact_path,
                    runtime=kwargs.get("runtime", "py3"),
                    entry_module=kwargs.get("entry_module", ""),
                    entry_callable=kwargs.get("entry_callable", "run"),
                    package_format=kwargs.get("package_format", ""),
                    dependency_allowlist=kwargs.get("dependency_allowlist"),
                    managed_global_names=kwargs.get("managed_global_names"),
                    worker_count=max(1, int(payload.get("pool_worker_count", payload.get("worker_count", 1)) or 1)),
                    heartbeat_timeout_sec=max(5, int(payload.get("pool_heartbeat_timeout_sec", 30) or 30)),
                    idle_ttl_sec=max(0, int(payload.get("pool_idle_ttl_sec", 0) or 0)),
                    healthy_only=kwargs.get("healthy_only", True),
                    tags=kwargs.get("tags"),
                    node_ids=kwargs.get("node_ids"),
                    node_count=max(0, int(payload.get("pool_node_count", kwargs.get("node_count", 0) or 0) or 0)),
                    node_limit=kwargs.get("node_limit", 100),
                    timeout_sec=kwargs.get("timeout_sec", 10.0),
                )
            with self._lock:
                self._current_executor = executor
            submit_resp = executor.submit_payloads(
                subtasks,
                job_id=job_id_snapshot,
                timeout_hint_sec=int(payload.get("timeout_hint_sec", 0) or 0),
                priority=max(1, int(payload.get("task_priority", 1) or 1)),
                runtime_key=str(payload.get("runtime_key", "") or "").strip(),
            )
            pending = {str(item.task_id or "").strip() for item in submit_resp.accepted}
            results: List[pb2.TaskResult] = []
            deadline = time.monotonic() + float(payload.get("wait_timeout_sec", 3600.0) or 3600.0)
            chunk_timeout = max(0.5, min(10.0, float(payload.get("wait_chunk_timeout_sec", 5.0) or 5.0)))
            if pending and not hasattr(executor, "iter_results") and hasattr(executor, "wait_for_results"):
                results = list(
                    executor.wait_for_results(
                        expected_count=len(pending),
                        timeout_sec=max(0.1, deadline - time.monotonic()),
                        wait_ms=int(payload.get("wait_ms", 500) or 500),
                        job_id=job_id_snapshot,
                    )
                )
                pending.clear()
            while pending:
                if time.monotonic() >= deadline:
                    raise TimeoutError(f"job wait timeout with pending={len(pending)}")
                with self._lock:
                    state = self._jobs.get(job_id)
                    cancel_requested = bool(state.cancel_requested) if state is not None else False
                if cancel_requested:
                    try:
                        executor.cancel_job(reason="job queue cancel", job_id=job_id_snapshot)
                    except Exception:
                        pass
                    break
                batch = list(
                    executor.iter_results(
                        max_count=min(len(pending), int(payload.get("result_limit", 100) or 100)),
                        timeout_sec=chunk_timeout,
                        wait_ms=int(payload.get("wait_ms", 500) or 500),
                        job_id=job_id_snapshot,
                    )
                )
                for item in batch:
                    tid = str(item.task_id or "").strip()
                    if tid in pending:
                        pending.discard(tid)
                    results.append(item)
                if not batch:
                    time.sleep(0.05)
            rendered_results = [_task_result_to_dict(item) for item in results]
            with self._lock:
                state = self._jobs.get(job_id)
                if state is None:
                    return
                state.results = rendered_results
                state.finished_at = utc_now()
                if state.cancel_requested:
                    state.status = "CANCELLED"
                elif any(item.status != pb2.TASK_STATUS_SUCCEEDED for item in results):
                    state.status = "FAILED"
                    state.error = "one or more subtasks failed"
                else:
                    state.status = "SUCCEEDED"
        except Exception as exc:
            with self._lock:
                state = self._jobs.get(job_id)
                if state is not None:
                    state.status = "FAILED"
                    state.finished_at = utc_now()
                    state.error = str(exc)
        finally:
            if executor is not None:
                try:
                    executor.close()
                except Exception:
                    pass
