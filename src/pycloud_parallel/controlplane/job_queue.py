from __future__ import annotations

import base64
import contextlib
import gc
import hashlib
import json
import os
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
import tempfile
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional, Sequence

from pycloud_parallel.controlplane.data_registry import DataRegistryClient
from pycloud_parallel.controlplane.config import JOB_STAGED_REF_TTL_SEC, get_payload_policy
from pycloud_parallel.controlplane.data_ref import DataRef, maybe_data_ref
from pycloud_parallel.controlplane.infocenter_client import InfoCenterClient
from pycloud_parallel.controlplane.node_control_client import NodeControlClient
from pycloud_parallel.data.object_ref import NodeStoredRef
from pycloud_parallel.controlplane.payload_transport import normalize_inbound_payload
from pycloud_parallel.controlplane.serialization import convert_dict_to_arrow
from pycloud_parallel.controlplane.node.execution import (
    _invoke_user_callable,
    _load_user_module,
    _purge_loaded_artifact_modules,
)
from pycloud_parallel.execution.task_pool import TaskPool
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


def _create_job_task_pool(**kwargs: Any) -> TaskPool:
    return TaskPool.from_infocenter(**kwargs)


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


def _payload_object_ref(value: object) -> Optional[NodeStoredRef | DataRef]:
    if isinstance(value, NodeStoredRef):
        return value
    return maybe_data_ref(value)


_JOB_DELAYED_RESOLVE_SKIP_KEYS = {
    "blob_ref",
    "blob_b64",
    "blob_control_addr",
    "driver_blob_ref",
    "driver_blob_b64",
    "driver_blob_control_addr",
}


def _should_skip_delayed_resolve_key(*, path: str, key: str) -> bool:
    return str(path or "").strip() == "payload" and str(key or "").strip() in _JOB_DELAYED_RESOLVE_SKIP_KEYS


def _collect_payload_data_ref_ids(value: object) -> List[str]:
    out: List[str] = []
    seen: set[str] = set()

    def _walk(item: object, *, path: str = "payload") -> None:
        ref = maybe_data_ref(item)
        if ref is not None:
            ref_id = str(ref.ref_id or "").strip()
            if ref_id and ref_id not in seen:
                seen.add(ref_id)
                out.append(ref_id)
            return
        if isinstance(item, dict):
            for key, nested in item.items():
                normalized_key = str(key)
                if _should_skip_delayed_resolve_key(path=path, key=normalized_key):
                    continue
                _walk(nested, path=f"{path}.{normalized_key}")
            return
        if isinstance(item, (list, tuple)):
            for idx, nested in enumerate(item):
                _walk(nested, path=f"{path}[{idx}]")

    _walk(value)
    return out


def _validate_delayed_resolve_refs(value: object) -> None:
    def _walk(item: object, *, path: str = "payload") -> None:
        ref = maybe_data_ref(item)
        if ref is not None:
            locator_kind = str(ref.locator_kind or "").strip().lower()
            locator_token = str(ref.locator_token or "").strip()
            control_addr = str(ref.control_addr or "").strip()
            if locator_kind == "node_local" and not locator_token and not control_addr:
                raise ValueError(
                    f"{path} contains a large-data reference without a resolvable locator; "
                    "use controlplane staging for business payload data"
                )
            if locator_kind == "node_control" and not locator_token and not control_addr:
                raise ValueError(f"{path} contains a node_control DataRef without control_addr")
            return
        if isinstance(item, dict):
            for key, nested in item.items():
                normalized_key = str(key)
                if _should_skip_delayed_resolve_key(path=path, key=normalized_key):
                    continue
                _walk(nested, path=f"{path}.{normalized_key}")
            return
        if isinstance(item, (list, tuple)):
            for idx, nested in enumerate(item):
                _walk(nested, path=f"{path}[{idx}]")

    _walk(value)


def _resolve_payload_data_refs(
    value: object,
    *,
    registry_target: str,
    timeout_sec: float = 10.0,
) -> object:
    registry = DataRegistryClient(registry_target, timeout_sec=max(0.1, float(timeout_sec)))

    def _resolve(item: object, *, path: str = "payload") -> object:
        data_ref = maybe_data_ref(item)
        if data_ref is not None:
            last_exc: Optional[Exception] = None
            for backoff in (0.0, 0.5, 1.0, 2.0):
                if backoff > 0.0:
                    time.sleep(backoff)
                try:
                    resolved = registry.resolve(data_ref)
                except Exception as exc:
                    last_exc = exc
                    continue
                replicas = [
                    dict(candidate)
                    for candidate in getattr(resolved, "replicas", ()) or ()
                    if isinstance(candidate, dict) and str(candidate.get("control_addr", "") or "").strip()
                ]
                if not replicas and str(resolved.control_addr or "").strip():
                    replicas = [
                        {
                            "control_addr": str(resolved.control_addr or "").strip(),
                            "node_id": str(resolved.node_id or "").strip(),
                            "node_instance_id": str(resolved.node_instance_id or "").strip(),
                        }
                    ]
                for replica in replicas:
                    control_addr = str(replica.get("control_addr", "") or "").strip()
                    if not control_addr:
                        continue
                    try:
                        with NodeControlClient(control_addr, timeout_sec=max(0.1, float(timeout_sec))) as client:
                            return client.fetch_result_ref_data(data_ref)
                    except Exception as exc:
                        last_exc = exc
                        continue
            if last_exc is None:
                raise RuntimeError(
                    f"staging data unavailable for ref_id={data_ref.ref_id}: no readable replica"
                )
            raise RuntimeError(
                f"staging data unavailable for ref_id={data_ref.ref_id}: {last_exc}"
            )
        if isinstance(item, dict):
            out: Dict[str, object] = {}
            for key, value in item.items():
                normalized_key = str(key)
                if _should_skip_delayed_resolve_key(path=path, key=normalized_key):
                    out[normalized_key] = value
                else:
                    out[normalized_key] = _resolve(value, path=f"{path}.{normalized_key}")
            return out
        if isinstance(item, list):
            return [_resolve(nested, path=f"{path}[{idx}]") for idx, nested in enumerate(item)]
        if isinstance(item, tuple):
            return tuple(_resolve(nested, path=f"{path}[{idx}]") for idx, nested in enumerate(item))
        return item

    return _resolve(value)


def _resolve_job_blob_bytes(
    payload: Dict[str, object],
    *,
    b64_key: str,
    ref_key: str,
    control_addr_key: str,
) -> Optional[bytes]:
    blob_b64 = str(payload.get(b64_key, "") or "").strip()
    if blob_b64:
        return base64.b64decode(blob_b64.encode("utf-8"))

    ref_value = payload.get(ref_key)
    if isinstance(ref_value, (bytes, bytearray, memoryview)):
        return bytes(ref_value)
    if isinstance(ref_value, os.PathLike):
        return Path(ref_value).read_bytes()
    if isinstance(ref_value, str):
        path = Path(ref_value).expanduser()
        if path.exists() and path.is_file():
            return path.read_bytes()

    ref = _payload_object_ref(ref_value)
    if ref is None:
        return None
    control_addr = str(payload.get(control_addr_key, "") or "").strip()
    if not control_addr:
        raise RuntimeError(f"{ref_key} is missing {control_addr_key}")
    timeout_sec = max(0.1, float(payload.get("timeout_sec", 10.0) or 10.0))
    with NodeControlClient(control_addr, timeout_sec=timeout_sec) as client:
        return client.download_object_bytes(object_id=ref.object_id)


def _job_queue_object_dir() -> Path:
    custom = str(os.environ.get("PYCLOUD_JOB_QUEUE_OBJECT_DIR", "") or "").strip()
    if custom:
        return Path(custom).expanduser().resolve()
    home = str(os.environ.get("PYCLOUD_HOME", "") or "").strip()
    if home:
        return Path(home).expanduser().resolve() / "code_cache" / "objects"
    return (Path.cwd() / "code_cache" / "objects").resolve()


def _resolve_job_hook_mapping(
    value: object,
    *,
    module: Any,
    label: str,
    payload: Dict[str, object],
) -> Dict[str, object]:
    prepared = value
    if isinstance(prepared, str):
        callable_name = str(prepared or "").strip()
        if callable_name:
            candidate = getattr(module, callable_name, None)
            if candidate is None or not callable(candidate):
                raise RuntimeError(f"{label} callable not found: {callable_name}")
            prepared = _invoke_user_callable(candidate, payload)
    elif callable(prepared):
        prepared = _invoke_user_callable(prepared, payload)

    if prepared is None:
        return {}
    if not isinstance(prepared, dict):
        raise RuntimeError(f"{label} must be dict or callable returning dict")
    return dict(prepared)


def _normalize_job_payload(value: object) -> Dict[str, object]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise RuntimeError("job_payload must be dict")
    return dict(value)


def _resolve_task_generator_output(
    task_generator_spec: object,
    *,
    module: Any,
    payload: Dict[str, object],
) -> Any:
    produced = task_generator_spec
    if isinstance(produced, str):
        callable_name = str(produced or "").strip() or "task_generator"
        candidate = getattr(module, callable_name, None)
        if candidate is None or not callable(candidate):
            raise RuntimeError(f"task generator callable not found: {callable_name}")
        produced = _invoke_user_callable(candidate, payload)
    elif callable(produced):
        produced = _invoke_user_callable(produced, payload)

    if isinstance(produced, dict) and isinstance(produced.get("payloads"), list):
        produced = produced["payloads"]
    elif isinstance(produced, dict) and isinstance(produced.get("subtasks"), list):
        produced = produced["subtasks"]
    if produced is None:
        raise RuntimeError("task_generator returned no payloads")
    if isinstance(produced, dict):
        raise RuntimeError("task_generator must return an iterable of dict payloads")
    return produced


def _preview_job_value(value: object, *, limit: int = 160) -> str:
    if value is None:
        return ""
    try:
        text = json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=str)
    except Exception:
        text = repr(value)
    normalized = " ".join(str(text or "").split())
    if len(normalized) <= max(16, int(limit)):
        return normalized
    return normalized[: max(13, int(limit) - 3)] + "..."


def _uses_job_hooks(payload: Dict[str, object]) -> bool:
    if str(payload.get("job_mode", "") or "").strip().lower() == "hooks":
        return True
    return any(
        str(payload.get(name, "") or "").strip()
        for name in (
            "task_generator_callable",
            "handle_result_callable",
            "finalize_callable",
            "update_globals",
        )
    )


def _auth_token_digest(token: str) -> str:
    normalized = str(token or "").strip()
    if not normalized:
        return ""
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _job_auth_ttl_sec() -> int:
    for key in ("PYCLOUD_JOB_AUTH_TTL_SEC", "PYCLOUD_JOB_CLIENT_AUTH_TTL_SEC"):
        raw = str(os.environ.get(key, "") or "").strip()
        if not raw:
            continue
        try:
            return max(60, int(raw))
        except Exception:
            continue
    return 24 * 60 * 60


def _default_job_worker_count() -> int:
    return max(1, int((os.cpu_count() or 1) * 0.5))


def _default_job_node_count(
    *,
    controlplane_target: str,
    payload: Dict[str, object],
) -> int:
    explicit_node_ids = [str(item).strip() for item in list(payload.get("node_ids") or ()) if str(item).strip()]
    if explicit_node_ids:
        return len(explicit_node_ids)

    runtime = str(payload.get("runtime", "py3") or "py3")
    tags = list(payload.get("tags") or ())
    node_limit = int(payload.get("node_limit", 100) or 100)
    try:
        with InfoCenterClient(controlplane_target, timeout_sec=float(payload.get("timeout_sec", 10.0) or 10.0)) as infocenter:
            selected = list(
                infocenter.select_task_nodes(
                    healthy_only=bool(payload.get("healthy_only", True)),
                    tags=tags,
                    node_ids=explicit_node_ids or None,
                    node_count=0,
                    limit=node_limit,
                    require_credit=False,
                    preferred_runtime_key=str(payload.get("preferred_runtime_key", "") or "").strip(),
                    runtime=runtime,
                )
            )
    except Exception:
        return 0
    return len(selected)


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
    final_result: object = None
    enqueue_seq: int = 0
    owner_token_digest: str = ""
    owner_token_expires_at: Optional[datetime] = None
    staged_ref_ids: List[str] = field(default_factory=list)
    last_ref_touch_at: Optional[datetime] = None
    payload_schema_version: int = 1

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
            "final_result": self.final_result,
            "enqueue_seq": int(self.enqueue_seq or 0),
            "staged_ref_ids": list(self.staged_ref_ids),
            "last_ref_touch_at": self.last_ref_touch_at.isoformat() if self.last_ref_touch_at else "",
            "payload_schema_version": int(self.payload_schema_version or 0),
        }


class JobQueueManager:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._cv = threading.Condition(self._lock)
        self._jobs: Dict[str, JobState] = {}
        self._waiting_order: List[str] = []
        self._enqueue_seq = 0
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
        with self._lock:
            executor = self._current_executor
            release_states = [state for state in self._jobs.values() if state.staged_ref_ids]
        if executor is not None:
            try:
                executor.close()
            except Exception:
                pass
        for state in release_states:
            self._release_job_refs(state)

    def submit_job(self, payload: Dict[str, object], *, auth_token: str = "") -> JobState:
        raw_payload = dict(payload or {})
        normalized_payload = normalize_inbound_payload(
            raw_payload,
            object_dir=str(_job_queue_object_dir()),
            policy=get_payload_policy("job_submit"),
            resolve_object_refs=lambda value: value,
        )
        if not isinstance(normalized_payload, dict):
            raise ValueError("job payload must resolve to a dict")
        _validate_delayed_resolve_refs(normalized_payload)
        job_id = str(normalized_payload.get("job_id", "") or "").strip() or f"jobq-{uuid.uuid4().hex}"
        client_id = str(normalized_payload.get("client_id", "") or "").strip() or f"job-client-{uuid.uuid4().hex[:8]}"
        priority = max(0, int(normalized_payload.get("priority", 0) or 0))
        owner_token_digest = _auth_token_digest(auth_token)
        submitted_at = utc_now()
        owner_token_expires_at = submitted_at + timedelta(seconds=_job_auth_ttl_sec()) if owner_token_digest else None
        staged_ref_ids = _collect_payload_data_ref_ids(normalized_payload)
        with self._cv:
            self._enqueue_seq += 1
            enqueue_seq = self._enqueue_seq
            state = JobState(
                job_id=job_id,
                client_id=client_id,
                priority=priority,
                status="WAITING",
                submitted_at=submitted_at,
                code_version=str(normalized_payload.get("code_version", "") or "").strip(),
                entry_module=str(normalized_payload.get("entry_module", "") or "").strip(),
                entry_callable=str(normalized_payload.get("entry_callable", "") or "run").strip() or "run",
                payload=dict(normalized_payload),
                enqueue_seq=enqueue_seq,
                owner_token_digest=owner_token_digest,
                owner_token_expires_at=owner_token_expires_at,
                staged_ref_ids=staged_ref_ids,
                last_ref_touch_at=submitted_at if staged_ref_ids else None,
                payload_schema_version=2 if staged_ref_ids else 1,
            )
            self._jobs[job_id] = state
            self._insert_waiting_job_locked(state)
            self._cv.notify_all()
        return state

    def get_job(self, job_id: str) -> Optional[JobState]:
        with self._lock:
            return self._jobs.get(str(job_id or "").strip())

    def summary(self, *, recent_limit: int = 5, waiting_limit: int = 50) -> Dict[str, object]:
        with self._lock:
            jobs = list(self._jobs.values())
            waiting = sum(1 for item in jobs if item.status == "WAITING" and not item.cancel_requested)
            running = sum(1 for item in jobs if item.status == "RUNNING")
            succeeded = sum(1 for item in jobs if item.status == "SUCCEEDED")
            failed = sum(1 for item in jobs if item.status == "FAILED")
            cancelled = sum(1 for item in jobs if item.status == "CANCELLED")
            current = str(self._running_job_id or "").strip()
            current_state = self._jobs.get(current) if current else None
            recent_jobs = sorted(
                jobs,
                key=lambda item: (
                    (item.finished_at or item.started_at or item.submitted_at).timestamp(),
                    item.submitted_at.timestamp(),
                    item.job_id,
                ),
                reverse=True,
            )[: max(0, int(recent_limit or 0))]
            waiting_jobs: List[Dict[str, object]] = []
            for position, job_id in enumerate(list(self._waiting_order)[: max(0, int(waiting_limit or 0))], start=1):
                item = self._jobs.get(job_id)
                if item is None or item.status != "WAITING" or item.cancel_requested:
                    continue
                waiting_jobs.append(
                    {
                        "job_id": str(item.job_id or ""),
                        "priority": int(item.priority or 0),
                        "submitted_at": item.submitted_at.isoformat(),
                        "position": position,
                    }
                )
            return {
                "job_count": len(jobs),
                "waiting": waiting,
                "running": running,
                "succeeded": succeeded,
                "failed": failed,
                "cancelled": cancelled,
                "terminal": succeeded + failed + cancelled,
                "current_job_id": current,
                "current_job_status": str(current_state.status) if current_state is not None else "",
                "recent_jobs": [
                    {
                        "job_id": str(item.job_id or ""),
                        "status": str(item.status or ""),
                        "submitted_at": item.submitted_at.isoformat(),
                        "finished_at": item.finished_at.isoformat() if item.finished_at else "",
                        "final_result_preview": _preview_job_value(item.final_result),
                        "error_preview": _preview_job_value(item.error),
                    }
                    for item in recent_jobs
                ],
                "waiting_jobs": waiting_jobs,
            }

    def cancel_job(self, job_id: str, *, auth_token: str = "") -> Optional[JobState]:
        normalized = str(job_id or "").strip()
        release_state: Optional[JobState] = None
        with self._cv:
            state = self._jobs.get(normalized)
            if state is None:
                return None
            if state.owner_token_digest:
                expires_at = state.owner_token_expires_at
                if expires_at is not None and utc_now() > expires_at:
                    raise PermissionError("cancel auth expired")
                provided_digest = _auth_token_digest(auth_token)
                if not provided_digest or provided_digest != state.owner_token_digest:
                    raise PermissionError("cancel auth failed")
            if state.status == "WAITING":
                state.status = "CANCELLED"
                state.finished_at = utc_now()
                state.cancel_requested = True
                self._remove_waiting_job_locked(state.job_id)
                release_state = JobState(**{**state.__dict__})
            elif state.status == "RUNNING":
                state.cancel_requested = True
                executor = self._current_executor
                if executor is not None and getattr(executor, "job_id", "") == state.job_id:
                    try:
                        executor.cancel_job(reason="job queue cancel", job_id=state.job_id)
                    except Exception:
                        pass
                return state
            else:
                return state
        if release_state is not None:
            self._release_job_refs(release_state)
            return self.get_job(normalized)
        return None

    def _pick_next_job_locked(self) -> Optional[JobState]:
        self._prune_waiting_order_locked()
        for job_id in self._waiting_order:
            job = self._jobs.get(job_id)
            if job is None or job.status != "WAITING" or job.cancel_requested:
                continue
            return job
        waiting = [job for job in self._jobs.values() if job.status == "WAITING" and not job.cancel_requested]
        if not waiting:
            return None
        waiting.sort(key=lambda item: (-int(item.priority), item.submitted_at.timestamp(), int(item.enqueue_seq or 0), item.job_id))
        self._waiting_order = [item.job_id for item in waiting]
        return waiting[0]

    def reorder_job(self, job_id: str, *, direction: str) -> Optional[JobState]:
        normalized = str(job_id or "").strip()
        move = str(direction or "").strip().lower()
        with self._cv:
            state = self._jobs.get(normalized)
            if state is None or state.status != "WAITING" or state.cancel_requested:
                return state
            self._prune_waiting_order_locked()
            if normalized not in self._waiting_order:
                self._insert_waiting_job_locked(state)
            idx = self._waiting_order.index(normalized)
            if move == "up" and idx > 0:
                self._waiting_order[idx - 1], self._waiting_order[idx] = self._waiting_order[idx], self._waiting_order[idx - 1]
            elif move == "down" and idx < len(self._waiting_order) - 1:
                self._waiting_order[idx + 1], self._waiting_order[idx] = self._waiting_order[idx], self._waiting_order[idx + 1]
            elif move not in {"up", "down"}:
                raise ValueError("direction must be 'up' or 'down'")
            self._cv.notify_all()
            return state

    def _remove_waiting_job_locked(self, job_id: str) -> None:
        normalized = str(job_id or "").strip()
        if not normalized:
            return
        self._waiting_order = [item for item in self._waiting_order if item != normalized]

    def _prune_waiting_order_locked(self) -> None:
        seen: set[str] = set()
        kept: List[str] = []
        for job_id in self._waiting_order:
            if job_id in seen:
                continue
            seen.add(job_id)
            state = self._jobs.get(job_id)
            if state is None or state.status != "WAITING" or state.cancel_requested:
                continue
            kept.append(job_id)
        self._waiting_order = kept

    def _insert_waiting_job_locked(self, state: JobState) -> None:
        self._prune_waiting_order_locked()
        insert_at = len(self._waiting_order)
        for idx, job_id in enumerate(self._waiting_order):
            other = self._jobs.get(job_id)
            if other is None:
                continue
            key_state = (-int(state.priority), state.submitted_at.timestamp(), int(state.enqueue_seq or 0), state.job_id)
            key_other = (-int(other.priority), other.submitted_at.timestamp(), int(other.enqueue_seq or 0), other.job_id)
            if key_state < key_other:
                insert_at = idx
                break
        self._waiting_order.insert(insert_at, state.job_id)

    def _cleanup_jobs_locked(self) -> List[JobState]:
        if self._retention_sec <= 0:
            return []
        now = utc_now()
        expired = []
        for job_id, state in self._jobs.items():
            if state.status not in ("SUCCEEDED", "FAILED", "CANCELLED"):
                continue
            finished_at = state.finished_at or state.submitted_at
            if (now - finished_at).total_seconds() > self._retention_sec:
                expired.append(job_id)
        released: List[JobState] = []
        for job_id in expired:
            self._remove_waiting_job_locked(job_id)
            state = self._jobs.pop(job_id, None)
            if state is not None:
                released.append(state)
        return released

    def _job_ref_touch_interval_sec(self, state: JobState) -> float:
        ttl_sec = max(
            1,
            int((state.payload or {}).get("staging_ttl_sec", JOB_STAGED_REF_TTL_SEC) or JOB_STAGED_REF_TTL_SEC),
        )
        return max(1.0, min(float(ttl_sec) / 3.0, 300.0))

    def _jobs_needing_ref_touch_locked(self, *, now: datetime) -> List[JobState]:
        out: List[JobState] = []
        for state in self._jobs.values():
            if state.status != "WAITING" or state.cancel_requested:
                continue
            if not state.staged_ref_ids:
                continue
            last_touch = state.last_ref_touch_at or state.submitted_at
            if (now - last_touch).total_seconds() >= self._job_ref_touch_interval_sec(state):
                out.append(state)
        return out

    def _touch_job_refs(self, state: JobState) -> None:
        if not self._controlplane_target or not state.staged_ref_ids:
            return
        client = DataRegistryClient(self._controlplane_target, timeout_sec=5.0)
        touched = False
        for ref_id in list(state.staged_ref_ids):
            try:
                client.touch(ref_id)
                touched = True
            except Exception:
                continue
        if touched:
            with self._lock:
                current = self._jobs.get(state.job_id)
                if current is not None:
                    current.last_ref_touch_at = utc_now()

    def _release_job_refs(self, state: JobState) -> None:
        if not self._controlplane_target or not state.staged_ref_ids:
            return
        client = DataRegistryClient(self._controlplane_target, timeout_sec=5.0)
        for ref_id in list(state.staged_ref_ids):
            with contextlib.suppress(Exception):
                client.release(ref_id)
        with self._lock:
            current = self._jobs.get(state.job_id)
            if current is not None:
                current.staged_ref_ids = []

    def _loop(self) -> None:
        while True:
            refs_to_touch: List[JobState] = []
            refs_to_release: List[JobState] = []
            next_job: Optional[JobState] = None
            with self._cv:
                while not self._stop and self._running_job_id:
                    self._cv.wait(timeout=0.1)
                if self._stop:
                    return
                refs_to_release = self._cleanup_jobs_locked()
                refs_to_touch = self._jobs_needing_ref_touch_locked(now=utc_now())
                next_job = self._pick_next_job_locked()
                if next_job is None:
                    self._cv.wait(timeout=0.1)
                else:
                    next_job.status = "RUNNING"
                    next_job.started_at = utc_now()
                    if next_job.checkpoint is None:
                        next_job.checkpoint = {}
                    next_job.checkpoint["phase"] = "preparing"
                    self._remove_waiting_job_locked(next_job.job_id)
                    self._running_job_id = next_job.job_id
            for state in refs_to_release:
                self._release_job_refs(state)
            for state in refs_to_touch:
                self._touch_job_refs(state)
            if next_job is None:
                continue
            self._run_job(next_job.job_id)
            terminal_state = self.get_job(next_job.job_id)
            if terminal_state is not None and terminal_state.status in {"SUCCEEDED", "FAILED", "CANCELLED"}:
                self._release_job_refs(terminal_state)
            with self._cv:
                self._running_job_id = ""
                self._current_executor = None
                self._cv.notify_all()

    def _expand_subtasks(self, payload: Dict[str, object]) -> List[Dict[str, object]]:
        subtasks = payload.get("subtasks") or []
        if isinstance(subtasks, list) and subtasks:
            return [dict(item) for item in subtasks if isinstance(item, dict)]

        driver_blob = _resolve_job_blob_bytes(
            payload,
            b64_key="driver_blob_b64",
            ref_key="driver_blob_ref",
            control_addr_key="driver_blob_control_addr",
        )
        if not driver_blob:
            return []

        package_format = str(payload.get("driver_package_format", "py") or "py").strip() or "py"
        entry_module = str(payload.get("driver_entry_module", "") or "").strip()
        entry_callable = str(payload.get("driver_entry_callable", "run") or "run").strip() or "run"
        driver_payload = dict(payload.get("driver_payload") or {})

        fd, tmp_name = tempfile.mkstemp(prefix="pycloud-job-driver-", suffix=_artifact_suffix(package_format))
        os.close(fd)
        tmp_path = Path(tmp_name)
        module = None
        fn = None
        produced = None
        try:
            tmp_path.write_bytes(driver_blob)
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
            produced = None
            fn = None
            module = None
            try:
                _purge_loaded_artifact_modules(
                    str(tmp_path),
                    entry_module=entry_module,
                    package_format=package_format,
                    dependency_path="",
                )
            except Exception:
                pass
            tmp_path.unlink(missing_ok=True)
            gc.collect()

    def _run_job(self, job_id: str) -> None:
        with self._lock:
            state = self._jobs.get(job_id)
            if state is None:
                return
            payload = dict(state.payload)
            client_id = state.client_id
            job_id_snapshot = state.job_id

        try:
            with self._lock:
                state = self._jobs.get(job_id)
                if state is not None:
                    state.checkpoint["phase"] = "resolving_refs"
            resolved_payload = _resolve_payload_data_refs(
                payload,
                registry_target=self._controlplane_target,
                timeout_sec=float(payload.get("timeout_sec", 10.0) or 10.0),
            )
            if isinstance(resolved_payload, dict):
                payload = dict(resolved_payload)
            with self._lock:
                state = self._jobs.get(job_id)
                if state is not None:
                    state.checkpoint["phase"] = "selecting_nodes"
        except Exception as exc:
            with self._lock:
                state = self._jobs.get(job_id)
                if state is not None:
                    state.status = "FAILED"
                    state.finished_at = utc_now()
                    state.error = str(exc)
            return

        if _uses_job_hooks(payload):
            self._run_job_with_hooks(
                job_id=job_id,
                payload=payload,
                client_id=client_id,
                job_id_snapshot=job_id_snapshot,
            )
            return

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
        prepared_update_globals = (
            dict(payload.get("update_globals"))
            if isinstance(payload.get("update_globals"), dict)
            else {}
        )
        if prepared_update_globals and not kwargs["managed_global_names"]:
            kwargs["managed_global_names"] = [
                str(name).strip()
                for name in prepared_update_globals.keys()
                if str(name).strip()
            ]

        code_version = str(payload.get("code_version", "") or "").strip()
        if code_version:
            kwargs["code_version"] = code_version
        else:
            blob = _resolve_job_blob_bytes(
                payload,
                b64_key="blob_b64",
                ref_key="blob_ref",
                control_addr_key="blob_control_addr",
            )
            if not blob:
                with self._lock:
                    state = self._jobs.get(job_id)
                    if state is not None:
                        state.status = "FAILED"
                        state.finished_at = utc_now()
                        state.error = "job must provide either code_version or blob_b64/blob_ref"
                return
            kwargs["blob"] = blob
        artifact_path = str(payload.get("artifact_path", "") or "").strip()
        default_worker_count = _default_job_worker_count()
        default_node_count = _default_job_node_count(
            controlplane_target=self._controlplane_target,
            payload=payload,
        )

        executor: Optional[Any] = None
        try:
            executor = _create_job_task_pool(
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
                    worker_count=max(1, int(payload.get("pool_worker_count", payload.get("worker_count", default_worker_count)) or default_worker_count)),
                    heartbeat_timeout_sec=max(5, int(payload.get("pool_heartbeat_timeout_sec", 30) or 30)),
                    idle_ttl_sec=max(0, int(payload.get("pool_idle_ttl_sec", 0) or 0)),
                    healthy_only=kwargs.get("healthy_only", True),
                    tags=kwargs.get("tags"),
                    node_ids=kwargs.get("node_ids"),
                    node_count=max(0, int(payload.get("pool_node_count", kwargs.get("node_count", default_node_count) or default_node_count) or 0)),
                    node_limit=kwargs.get("node_limit", 100),
                    timeout_sec=kwargs.get("timeout_sec", 10.0),
                )
            with self._lock:
                self._current_executor = executor
            if prepared_update_globals:
                with self._lock:
                    state = self._jobs.get(job_id)
                    if state is not None:
                        state.checkpoint["phase"] = "fanout_globals"
                executor.update_globals(dict(prepared_update_globals))
            with self._lock:
                state = self._jobs.get(job_id)
                if state is not None:
                    state.checkpoint["phase"] = "running_tasks"
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

    def _run_job_with_hooks(
        self,
        *,
        job_id: str,
        payload: Dict[str, object],
        client_id: str,
        job_id_snapshot: str,
    ) -> None:
        blob = _resolve_job_blob_bytes(
            payload,
            b64_key="blob_b64",
            ref_key="blob_ref",
            control_addr_key="blob_control_addr",
        )
        if not blob:
            with self._lock:
                state = self._jobs.get(job_id)
                if state is not None:
                    state.status = "FAILED"
                    state.finished_at = utc_now()
                    state.error = "job hook mode requires blob_b64/blob_ref"
            return

        package_format = str(payload.get("package_format", "py") or "py").strip() or "py"
        entry_module = str(payload.get("entry_module", "") or "").strip()
        task_entry_callable = str(payload.get("entry_callable", "run") or "run").strip() or "run"
        raw_task_generator = payload.get("task_generator_callable", "task_generator")
        handle_result_callable = (
            str(payload.get("handle_result_callable", "") or "").strip()
        )
        finalize_callable = str(payload.get("finalize_callable", "") or "").strip()
        raw_job_payload = payload.get("job_payload")
        raw_update_globals = payload.get("update_globals")

        fd, tmp_name = tempfile.mkstemp(prefix="pycloud-job-hooks-", suffix=_artifact_suffix(package_format))
        os.close(fd)
        tmp_path = Path(tmp_name)
        executor: Optional[Any] = None
        module = None
        task_entry = None
        task_generator = None
        handle_result = None
        finalize = None
        produced = None
        stream = None
        try:
            tmp_path.write_bytes(blob)
            module = _load_user_module(
                str(tmp_path),
                entry_module=entry_module,
                package_format=package_format,
                dependency_path="",
            )
            task_entry = getattr(module, task_entry_callable, None)
            if task_entry is None or not callable(task_entry):
                raise RuntimeError(f"task entry callable not found: {task_entry_callable}")
            task_generator = raw_task_generator
            handle_result = None
            if handle_result_callable:
                handle_result = getattr(module, handle_result_callable, None)
                if handle_result is None or not callable(handle_result):
                    raise RuntimeError(f"handle_result callable not found: {handle_result_callable}")
            finalize = None
            if finalize_callable:
                finalize = getattr(module, finalize_callable, None)
                if finalize is None or not callable(finalize):
                    raise RuntimeError(f"finalize callable not found: {finalize_callable}")

            hook_kwargs = _normalize_job_payload(raw_job_payload)
            hook_kwargs.setdefault("job_id", job_id_snapshot)
            hook_kwargs.setdefault("client_id", client_id)
            prepared_update_globals = _resolve_job_hook_mapping(
                raw_update_globals,
                module=module,
                label="update_globals",
                payload=hook_kwargs,
            )
            effective_managed_global_names = list(payload.get("managed_global_names") or ())
            if not effective_managed_global_names and isinstance(prepared_update_globals, dict):
                effective_managed_global_names = [
                    str(name).strip()
                    for name in prepared_update_globals.keys()
                    if str(name).strip()
                ]
            default_worker_count = _default_job_worker_count()
            default_node_count = _default_job_node_count(
                controlplane_target=self._controlplane_target,
                payload=payload,
            )

            executor = _create_job_task_pool(
                infocenter_target=self._controlplane_target,
                job_id=job_id_snapshot,
                owner_client_id=client_id,
                pool_name=str(payload.get("pool_name", "") or f"job-pool-{job_id_snapshot}"),
                entry_func=task_entry,
                runtime=str(payload.get("runtime", "py3") or "py3"),
                dependency_allowlist=list(payload.get("dependency_allowlist") or ()),
                managed_global_names=effective_managed_global_names,
                worker_count=max(1, int(payload.get("pool_worker_count", payload.get("worker_count", default_worker_count)) or default_worker_count)),
                heartbeat_timeout_sec=max(5, int(payload.get("pool_heartbeat_timeout_sec", 30) or 30)),
                idle_ttl_sec=max(0, int(payload.get("pool_idle_ttl_sec", 0) or 0)),
                healthy_only=bool(payload.get("healthy_only", True)),
                tags=list(payload.get("tags") or ()),
                node_ids=list(payload.get("node_ids") or ()),
                node_count=max(0, int(payload.get("pool_node_count", payload.get("node_count", default_node_count) or default_node_count) or 0)),
                node_limit=int(payload.get("node_limit", 100) or 100),
                timeout_sec=float(payload.get("timeout_sec", 10.0) or 10.0),
            )
            with self._lock:
                self._current_executor = executor

            if prepared_update_globals:
                with self._lock:
                    current_state = self._jobs.get(job_id)
                    if current_state is not None:
                        current_state.checkpoint["phase"] = "fanout_globals"
                executor.update_globals(dict(prepared_update_globals))

            produced = _resolve_task_generator_output(
                task_generator,
                module=module,
                payload=hook_kwargs,
            )

            def _payload_stream() -> Any:
                for item in produced:
                    if not isinstance(item, dict):
                        raise RuntimeError("task_generator must yield dict payloads")
                    yield dict(item)

            state_obj: object = payload.get("initial_state")
            if state_obj is None:
                state_obj = {"results": []}
            rendered_results: List[Dict[str, object]] = []
            with self._lock:
                current_state = self._jobs.get(job_id)
                if current_state is not None:
                    current_state.checkpoint["phase"] = "running_tasks"
            stream = executor.unordered(
                _payload_stream(),
                max_in_flight=max(1, int(payload.get("max_in_flight", 100) or 100)),
                receive_batch=max(1, int(payload.get("receive_batch", 10) or 10)),
                submit_timeout_sec=float(payload.get("submit_timeout_sec", 60.0) or 60.0),
                result_timeout_sec=float(payload.get("result_timeout_sec", payload.get("wait_chunk_timeout_sec", 30.0)) or 30.0),
                wait_ms=int(payload.get("wait_ms", 500) or 500),
                raise_on_error=True,
                node_window_factor=float(payload.get("node_window_factor", 2.0) or 2.0),
            )
            for task_id, result in stream:
                with self._lock:
                    current_state = self._jobs.get(job_id)
                    cancel_requested = bool(current_state.cancel_requested) if current_state is not None else False
                if cancel_requested:
                    try:
                        executor.cancel_job(reason="job queue cancel", job_id=job_id_snapshot)
                    except Exception:
                        pass
                    break
                rendered_results.append(
                    {
                        "task_id": str(task_id),
                        "job_id": job_id_snapshot,
                        "status": int(pb2.TASK_STATUS_SUCCEEDED),
                        "status_text": pb2.TaskStatus.Name(pb2.TASK_STATUS_SUCCEEDED),
                        "attempt": 1,
                        "result": result,
                    }
                )
                if handle_result is None:
                    if isinstance(state_obj, dict):
                        state_obj.setdefault("results", [])
                        results_bucket = state_obj.get("results")
                        if isinstance(results_bucket, list):
                            results_bucket.append({"task_id": str(task_id), "result": result})
                else:
                    returned_state = handle_result(
                        str(task_id),
                        result,
                        state=state_obj,
                        job_payload=dict(hook_kwargs),
                        job_id=job_id_snapshot,
                        client_id=client_id,
                    )
                    if returned_state is not None:
                        state_obj = returned_state
                with self._lock:
                    current_state = self._jobs.get(job_id)
                    if current_state is not None:
                        current_state.results = list(rendered_results)
                        current_state.checkpoint = {
                            "processed": len(rendered_results),
                            "current_task_id": str(task_id),
                        }

            final_result = state_obj
            if finalize is not None:
                finalized = finalize(
                    state=state_obj,
                    job_payload=dict(hook_kwargs),
                    job_id=job_id_snapshot,
                    client_id=client_id,
                )
                if finalized is not None:
                    final_result = finalized

            with self._lock:
                state = self._jobs.get(job_id)
                if state is None:
                    return
                state.results = list(rendered_results)
                state.final_result = final_result
                state.finished_at = utc_now()
                if state.cancel_requested:
                    state.status = "CANCELLED"
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
            stream = None
            produced = None
            finalize = None
            handle_result = None
            task_generator = None
            task_entry = None
            module = None
            try:
                _purge_loaded_artifact_modules(
                    str(tmp_path),
                    entry_module=entry_module,
                    package_format=package_format,
                    dependency_path="",
                )
            except Exception:
                pass
            tmp_path.unlink(missing_ok=True)
            gc.collect()
            if executor is not None:
                try:
                    executor.close()
                except Exception:
                    pass
