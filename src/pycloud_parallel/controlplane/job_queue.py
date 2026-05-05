from __future__ import annotations

import base64
import contextlib
import gc
import hashlib
import json
import os
import logging
import shutil
import threading
import time
import warnings
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
import tempfile
from concurrent.futures import ThreadPoolExecutor
import math
from typing import Any, Dict, List, Optional, Sequence

from pycloud_parallel.controlplane.artifact import Artifact, ArtifactDeps
from pycloud_parallel.controlplane.data_registry import DataRegistryClient
from pycloud_parallel.controlplane.config import JOB_STAGED_REF_TTL_SEC, get_jobqueue_resolve_refs, get_payload_policy
from pycloud_parallel.data.ref import DataRef, maybe_data_ref
from pycloud_parallel.controlplane.effective_policy import resolve_effective_policy
from pycloud_parallel.controlplane.infocenter_client import InfoCenterClient
from pycloud_parallel.controlplane.node_control_client import NodeControlClient
from pycloud_parallel.controlplane.policy_profile import (
    get_default_mode_for_binding,
    get_default_policy_id_for_binding,
    get_policy_profile,
)
from pycloud_parallel.controlplane.payload_transport import normalize_inbound_payload
from pycloud_parallel.controlplane.serialization import (
    convert_dict_to_arrow,
)
from pycloud_parallel.controlplane.serialization_mode import resolve_effective_serialization_mode
from pycloud_parallel.controlplane.node.execution import (
    _invoke_local_user_callable,
    _load_user_module,
    _purge_loaded_artifact_modules,
)
from pycloud_parallel.execution.task_pool import TaskPool
from pycloud_parallel.proto.v1 import pycloud_v1_pb2 as pb2

logger = logging.getLogger(__name__)


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
    data = dict(kwargs or {})
    target = str(data.pop("target", "") or data.pop("infocenter_target", "") or "").strip()
    source = data.pop("source", None)
    entry_module = str(data.pop("entry_module", "") or "").strip()
    entry_callable = str(data.pop("entry_callable", "") or "run").strip() or "run"
    package_format = str(data.pop("package_format", "") or "").strip()
    runtime = str(data.get("runtime", "py3") or "py3")
    managed_global_names = tuple(data.get("managed_global_names") or ())
    deps = data.get("deps")
    if source is not None and "artifact" not in data:
        if isinstance(source, (bytes, bytearray, memoryview)):
            if not package_format:
                package_format = "py"
            data["artifact"] = Artifact.from_bytes(
                bytes(source),
                package_format=package_format,
                runtime=runtime,
                entry_module=entry_module,
                entry_callable=entry_callable,
                deps=deps,
                managed_global_names=managed_global_names,
            )
        else:
            data["source"] = source
    return TaskPool.open(target=target, **data)


_JOB_ORCH_TASKPOOL_BINDING_ID = "taskpool_default"
_DEFAULT_JOB_QUEUE_POOL_IDLE_TTL_SEC = 300


def _close_executor(executor: Any) -> None:
    if executor is None:
        return
    try:
        executor.close()
    except Exception:
        pass


_TASKPOOL_SHARED_BINDING_ID = "taskpool_default"
_TASKPOOL_SHARED_POLICY_ID = get_default_policy_id_for_binding(_TASKPOOL_SHARED_BINDING_ID)
_TASKPOOL_SHARED_DEFAULT_MODE = get_default_mode_for_binding(_TASKPOOL_SHARED_BINDING_ID)


@dataclass
class _SharedPoolState:
    executor: TaskPool
    artifact_key: str
    policy_id: str
    current_mode: str
    last_used_at: datetime


@dataclass(frozen=True)
class _JobSharedPoolRequest:
    raw_requested_mode: str
    requested_mode: str
    reset_pool: bool
    default_worker_count: int
    default_node_count: int


@dataclass(frozen=True)
class _JobTaskPoolSpec:
    artifact_key: str
    create_pool: Any


def _payload_object_ref(value: object) -> Optional[DataRef]:
    return maybe_data_ref(value)


_JOB_DELAYED_RESOLVE_SKIP_KEYS = {
    "blob_ref",
    "blob_b64",
    "blob_control_addr",
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
    if get_jobqueue_resolve_refs() == "defer_to_worker":
        _validate_delayed_resolve_refs(value)
        return value

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
    from pycloud_parallel.controlplane.node_object_http import make_node_object_client

    with make_node_object_client(control_addr, timeout_sec=timeout_sec) as client:
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
            prepared = _invoke_local_user_callable(candidate, payload)
    elif callable(prepared):
        prepared = _invoke_local_user_callable(prepared, payload)

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
        produced = _invoke_local_user_callable(candidate, payload)
    elif callable(produced):
        produced = _invoke_local_user_callable(produced, payload)

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


def _ms(delta_sec: float) -> float:
    return round(max(0.0, float(delta_sec or 0.0)) * 1000.0, 3)


def _artifact_key_preview(value: str) -> str:
    text = str(value or "").strip()
    if len(text) <= 24:
        return text
    return text[:24] + "..."


def _default_timing_summary() -> Dict[str, object]:
    return {
        "queue_wait_ms": 0.0,
        "resolve_refs_ms": 0.0,
        "select_nodes_ms": 0.0,
        "pool_prepare_ms": 0.0,
        "executor_create_ms": 0.0,
        "executor_rebuild_ms": 0.0,
        "warmup_ms": 0.0,
        "fanout_globals_ms": 0.0,
        "running_tasks_ms": 0.0,
        "first_result_wait_ms": 0.0,
        "finalize_ms": 0.0,
        "terminal_writeback_ms": 0.0,
        "total_ms": 0.0,
        "executor_create_count": 0,
        "executor_rebuild_count": 0,
        "pool_reuse_count": 0,
        "result_count": 0,
        "task_count": 0,
        "pool_action": "",
    }


def _resolve_job_executor_max_in_flight(executor: object, requested: object) -> int:
    if requested is not None:
        try:
            normalized = int(requested)
        except Exception:
            normalized = 0
        if normalized > 0:
            return max(1, normalized)
    pools = getattr(executor, "_pools", None)
    if isinstance(pools, dict) and pools:
        total_workers = sum(
            max(0, int(getattr(pool, "worker_count", 0) or 0))
            for pool in pools.values()
        )
        if total_workers > 0:
            return max(1, int(math.ceil(float(total_workers) * 1.5)))
    resolver = getattr(executor, "_resolve_max_in_flight", None)
    if callable(resolver):
        try:
            return max(1, int(resolver(None)))
        except Exception:
            pass
    worker_count = max(0, int(getattr(executor, "worker_count", 0) or 0))
    return max(1, int(worker_count or 1))


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
    timing: Dict[str, object] = field(default_factory=_default_timing_summary)
    final_result_preview: str = ""
    error_preview: str = ""
    _timing_marks: Dict[str, float] = field(default_factory=dict, repr=False)

    def as_dict(
        self,
        *,
        include_payload: bool = True,
        include_results: bool = True,
        include_final_result: bool = True,
    ) -> Dict[str, object]:
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
            "payload": dict(self.payload) if include_payload else {},
            "checkpoint": dict(self.checkpoint),
            "cancel_requested": bool(self.cancel_requested),
            "error": self.error,
            "error_preview": self.error_preview,
            "results": list(self.results) if include_results else [],
            "result_count": len(self.results),
            "final_result": self.final_result if include_final_result else None,
            "final_result_preview": self.final_result_preview,
            "enqueue_seq": int(self.enqueue_seq or 0),
            "staged_ref_ids": list(self.staged_ref_ids),
            "last_ref_touch_at": self.last_ref_touch_at.isoformat() if self.last_ref_touch_at else "",
            "payload_schema_version": int(self.payload_schema_version or 0),
            "timing": dict(self.timing or {}),
        }


class JobQueueManager:
    def __init__(
        self,
        *,
        taskpool_policy_id: str = "",
        pool_idle_ttl_sec: Optional[int] = None,
    ) -> None:
        self._lock = threading.Lock()
        self._cv = threading.Condition(self._lock)
        self._jobs: Dict[str, JobState] = {}
        self._waiting_order: List[str] = []
        self._enqueue_seq = 0
        self._running_job_id = ""
        self._current_executor: Any = None
        self._shared_pool: Optional[_SharedPoolState] = None
        self._controlplane_target = ""
        self._stop = False
        self._thread: Optional[threading.Thread] = None
        self._maintenance_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="jobq-maint")
        self._summary_cache: Optional[Dict[str, object]] = None
        self._summary_cache_revision: int = 0
        self._summary_cache_built_at_monotonic: float = 0.0
        self._summary_cache_ttl_sec = max(0.0, float(os.getenv("PYCLOUD_JOB_QUEUE_SUMMARY_CACHE_TTL_SEC", "1.0") or 1.0))
        self._retention_sec = max(60, int(os.getenv("PYCLOUD_JOB_QUEUE_RETENTION_SEC", "3600") or 3600))
        self._taskpool_policy_id = str(taskpool_policy_id or "").strip().lower() or _TASKPOOL_SHARED_POLICY_ID
        self._taskpool_profile = get_policy_profile(self._taskpool_policy_id)
        self._pool_idle_ttl_sec = max(
            0,
            int(
                pool_idle_ttl_sec
                if pool_idle_ttl_sec is not None
                else (
                    os.getenv("PYCLOUD_JOB_QUEUE_POOL_IDLE_TTL_SEC", str(_DEFAULT_JOB_QUEUE_POOL_IDLE_TTL_SEC))
                    or _DEFAULT_JOB_QUEUE_POOL_IDLE_TTL_SEC
                )
            ),
        )

    def _submit_executor_close(self, executor: Any) -> None:
        if executor is None:
            return
        try:
            self._maintenance_executor.submit(_close_executor, executor)
        except RuntimeError:
            _close_executor(executor)

    def _job_state_locked(self, job_id: str) -> Optional[JobState]:
        return self._jobs.get(str(job_id or "").strip())

    def _invalidate_summary_locked(self) -> None:
        self._summary_cache_revision += 1
        self._summary_cache = None

    def _job_timing_mark_locked(self, job_id: str, mark: str) -> None:
        state = self._job_state_locked(job_id)
        if state is None:
            return
        state._timing_marks[str(mark)] = time.monotonic()

    def _job_timing_finish_locked(self, job_id: str, mark: str, metric_key: str) -> float:
        state = self._job_state_locked(job_id)
        if state is None:
            return 0.0
        started = state._timing_marks.pop(str(mark), None)
        if started is None:
            return 0.0
        elapsed_ms = _ms(time.monotonic() - started)
        timing = state.timing if isinstance(state.timing, dict) else _default_timing_summary()
        state.timing = timing
        timing[str(metric_key)] = elapsed_ms
        self._invalidate_summary_locked()
        return elapsed_ms

    def _job_timing_finalize_locked(self, job_id: str) -> None:
        state = self._job_state_locked(job_id)
        if state is None:
            return
        submitted_mark = state._timing_marks.get("submitted_at_monotonic")
        if submitted_mark is None:
            return
        timing = state.timing if isinstance(state.timing, dict) else _default_timing_summary()
        state.timing = timing
        timing["total_ms"] = _ms(time.monotonic() - submitted_mark)
        self._invalidate_summary_locked()

    def _refresh_job_previews_locked(self, job_id: str) -> None:
        state = self._job_state_locked(job_id)
        if state is None:
            return
        state.error_preview = _preview_job_value(state.error)
        state.final_result_preview = _preview_job_value(state.final_result)
        self._invalidate_summary_locked()

    def _aggregate_job_timing_locked(self, jobs: Sequence[JobState]) -> Dict[str, object]:
        numeric_keys = [
            "queue_wait_ms",
            "resolve_refs_ms",
            "select_nodes_ms",
            "pool_prepare_ms",
            "executor_create_ms",
            "executor_rebuild_ms",
            "warmup_ms",
            "fanout_globals_ms",
            "running_tasks_ms",
            "first_result_wait_ms",
            "finalize_ms",
            "terminal_writeback_ms",
            "total_ms",
        ]
        out: Dict[str, object] = {
            "job_count": 0,
            "executor_create_count": 0,
            "executor_rebuild_count": 0,
            "pool_reuse_count": 0,
            "pool_create_count": 0,
            "pool_rebuild_count": 0,
            "max_total_ms": 0.0,
        }
        sums = {key: 0.0 for key in numeric_keys}
        counted = 0
        for job in jobs:
            timing = dict(job.timing or {})
            if not timing:
                continue
            counted += 1
            for key in numeric_keys:
                sums[key] += float(timing.get(key, 0.0) or 0.0)
            out["executor_create_count"] = int(out["executor_create_count"]) + int(timing.get("executor_create_count", 0) or 0)
            out["executor_rebuild_count"] = int(out["executor_rebuild_count"]) + int(timing.get("executor_rebuild_count", 0) or 0)
            out["pool_reuse_count"] = int(out["pool_reuse_count"]) + (1 if str(timing.get("pool_action", "") or "") == "reuse" else 0)
            out["pool_create_count"] = int(out["pool_create_count"]) + (1 if str(timing.get("pool_action", "") or "") == "create" else 0)
            out["pool_rebuild_count"] = int(out["pool_rebuild_count"]) + (1 if str(timing.get("pool_action", "") or "") == "rebuild" else 0)
            out["max_total_ms"] = round(max(float(out["max_total_ms"] or 0.0), float(timing.get("total_ms", 0.0) or 0.0)), 3)
        out["job_count"] = counted
        for key, total in sums.items():
            out[f"avg_{key}"] = round((total / counted), 3) if counted > 0 else 0.0
        return out

    def start(self, *, controlplane_target: str) -> None:
        with self._lock:
            self._controlplane_target = str(controlplane_target or "").strip()
            if self._thread is not None and self._thread.is_alive():
                return
            self._stop = False
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
        self._thread = None
        with self._lock:
            executor = self._current_executor
            shared_executor = self._shared_pool.executor if self._shared_pool is not None else None
            self._shared_pool = None
            release_states = [state for state in self._jobs.values() if state.staged_ref_ids]
        self._submit_executor_close(executor)
        if shared_executor is not None and shared_executor is not executor:
            self._submit_executor_close(shared_executor)
        for state in release_states:
            self._release_job_refs(state)
        self._maintenance_executor.shutdown(wait=False, cancel_futures=True)

    def _resolve_requested_task_mode(self, payload: Dict[str, object]) -> str:
        requested_mode = str(payload.get("task_serialization_mode", "") or "").strip()
        effective = resolve_effective_policy(
            self._taskpool_profile,
            requested_mode=requested_mode or _TASKPOOL_SHARED_DEFAULT_MODE,
            context="taskpool_session",
        )
        return effective.resolved_mode

    def _resolve_job_shared_pool_request(self, payload: Dict[str, object]) -> _JobSharedPoolRequest:
        return _JobSharedPoolRequest(
            raw_requested_mode=str(payload.get("task_serialization_mode", "") or "").strip(),
            requested_mode=self._resolve_requested_task_mode(payload),
            reset_pool=bool(payload.get("reset_pool", False)),
            default_worker_count=_default_job_worker_count(),
            default_node_count=_default_job_node_count(
                controlplane_target=self._controlplane_target,
                payload=payload,
            ),
        )

    def _shared_pool_artifact_key(
        self,
        *,
        blob: Optional[bytes] = None,
        artifact_path: str = "",
        runtime: str = "",
        entry_module: str = "",
        entry_callable: str = "",
        package_format: str = "",
        dependency_allowlist: Sequence[str] = (),
        managed_global_names: Sequence[str] = (),
        task_resource_paths: Sequence[str] = (),
    ) -> str:
        if blob is not None:
            code_key = hashlib.sha256(bytes(blob)).hexdigest()
        elif artifact_path:
            code_key = f"path:{str(artifact_path).strip()}"
        else:
            code_key = f"module:{str(entry_module).strip()}"
        return self._artifact_key_from_code_key(
            code_key=code_key,
            runtime=runtime,
            entry_module=entry_module,
            entry_callable=entry_callable,
            package_format=package_format,
            dependency_allowlist=dependency_allowlist,
            managed_global_names=managed_global_names,
            task_resource_paths=task_resource_paths,
        )

    def _artifact_key_from_code_key(
        self,
        *,
        code_key: str,
        runtime: str,
        entry_module: str,
        entry_callable: str,
        package_format: str,
        dependency_allowlist: Sequence[str],
        managed_global_names: Sequence[str],
        task_resource_paths: Sequence[str],
    ) -> str:
        return "|".join(
            [
                str(code_key or "").strip(),
                str(runtime or "").strip(),
                str(entry_module or "").strip(),
                str(entry_callable or "").strip(),
                str(package_format or "").strip(),
                ",".join(sorted(str(item).strip() for item in (dependency_allowlist or ()) if str(item).strip())),
                ",".join(sorted(str(item).strip() for item in (managed_global_names or ()) if str(item).strip())),
                ",".join(sorted(str(item).strip() for item in (task_resource_paths or ()) if str(item).strip())),
            ]
        )

    def _switch_pool_mode_for_job(self, requested_mode: str, *, reset_pool: bool = False) -> None:
        shared = self._shared_pool
        if shared is None:
            raise RuntimeError("shared task pool is not initialized")
        pool = shared.executor
        pending = getattr(pool, "_pending_result_count", lambda: 0)()
        if int(pending or 0) > 0:
            raise RuntimeError("shared task pool still has pending results")
        if reset_pool:
            raise RuntimeError("shared task pool backend does not support reset_pool")
        effective = resolve_effective_policy(
            self._taskpool_profile,
            requested_mode=requested_mode or _TASKPOOL_SHARED_DEFAULT_MODE,
            context="taskpool_session",
        )
        pool._serialization_mode = effective.resolved_mode  # noqa: SLF001
        pool.effective_policy = effective
        pool.globals_digests = {}
        shared.current_mode = effective.resolved_mode
        shared.last_used_at = utc_now()
        logger.info(
            "[JobQueue] shared pool mode switch artifact_key=%s policy_id=%s mode=%s",
            _artifact_key_preview(shared.artifact_key),
            shared.policy_id,
            shared.current_mode,
        )

    def _close_shared_pool_locked(self) -> Any:
        shared = self._shared_pool
        self._shared_pool = None
        if shared is not None:
            logger.info(
                "[JobQueue] shared pool detached artifact_key=%s policy_id=%s mode=%s",
                _artifact_key_preview(shared.artifact_key),
                shared.policy_id,
                shared.current_mode,
            )
        return shared.executor if shared is not None else None

    def _detach_stale_running_locked(self) -> Any:
        current = str(self._running_job_id or "").strip()
        if not current:
            return None
        current_state = self._jobs.get(current)
        if current_state is not None and current_state.status not in {"SUCCEEDED", "FAILED", "CANCELLED"}:
            return None
        executor = self._current_executor
        shared_executor = self._shared_pool.executor if self._shared_pool is not None else None
        self._running_job_id = ""
        self._current_executor = None
        self._cv.notify_all()
        if shared_executor is not None and executor is shared_executor:
            return None
        return executor

    def _ensure_scheduler_thread_locked(self) -> None:
        if self._stop or not self._controlplane_target:
            return
        if self._thread is not None and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self._loop, name="job-queue-scheduler", daemon=True)
        self._thread.start()

    def _shared_pool_idle_expired_locked(self, *, now: datetime) -> bool:
        if self._shared_pool is None:
            return False
        if self._pool_idle_ttl_sec <= 0:
            return False
        return (now - self._shared_pool.last_used_at).total_seconds() >= float(self._pool_idle_ttl_sec)

    def _get_or_create_shared_pool(
        self,
        *,
        artifact_key: str,
        requested_mode: str,
        reset_pool: bool = False,
        create_pool: Any,
    ) -> TaskPool:
        pool_to_close = None
        with self._lock:
            shared = self._shared_pool
            if shared is not None and shared.artifact_key == artifact_key:
                try:
                    self._switch_pool_mode_for_job(requested_mode, reset_pool=reset_pool)
                    shared.last_used_at = utc_now()
                    logger.info(
                        "[JobQueue] shared pool reuse artifact_key=%s policy_id=%s mode=%s",
                        _artifact_key_preview(shared.artifact_key),
                        shared.policy_id,
                        shared.current_mode,
                    )
                    return shared.executor
                except Exception as exc:
                    logger.warning(
                        "job-orch shared pool soft switch failed; rebuilding pool "
                        "artifact_key=%s current_mode=%s requested_mode=%s policy_id=%s reset_pool=%s err=%r",
                        artifact_key,
                        shared.current_mode,
                        requested_mode,
                        shared.policy_id,
                        bool(reset_pool),
                        exc,
                    )
                    pool_to_close = self._close_shared_pool_locked()
            elif shared is not None:
                logger.info(
                    "[JobQueue] shared pool rebuild due to artifact change old=%s new=%s",
                    _artifact_key_preview(shared.artifact_key),
                    _artifact_key_preview(artifact_key),
                )
                pool_to_close = self._close_shared_pool_locked()
        if pool_to_close is not None:
            self._submit_executor_close(pool_to_close)

        pool = create_pool(requested_mode or _TASKPOOL_SHARED_DEFAULT_MODE)
        if not hasattr(pool, "unordered") and hasattr(pool, "imap_unordered"):
            try:
                setattr(pool, "unordered", getattr(pool, "imap_unordered"))
            except Exception:
                pass
        effective = resolve_effective_policy(
            self._taskpool_profile,
            requested_mode=requested_mode or _TASKPOOL_SHARED_DEFAULT_MODE,
            context="taskpool_session",
        )
        pool._serialization_mode = effective.resolved_mode  # noqa: SLF001
        pool.effective_policy = effective
        with self._lock:
            self._shared_pool = _SharedPoolState(
                executor=pool,
                artifact_key=artifact_key,
                policy_id=self._taskpool_policy_id,
                current_mode=effective.resolved_mode,
                last_used_at=utc_now(),
            )
        logger.info(
            "[JobQueue] shared pool create artifact_key=%s policy_id=%s mode=%s",
            _artifact_key_preview(artifact_key),
            self._taskpool_policy_id,
            effective.resolved_mode,
        )
        return pool

    def _record_pool_prepare_timing_locked(
        self,
        *,
        state: Optional[JobState],
        action: str,
        pool_prepare_ms: float,
    ) -> None:
        if state is None:
            return
        state.timing["pool_action"] = action
        state.timing["pool_prepare_ms"] = pool_prepare_ms
        state.timing["executor_create_ms"] = pool_prepare_ms if action == "create" else 0.0
        state.timing["executor_rebuild_ms"] = pool_prepare_ms if action == "rebuild" else 0.0
        state.timing["executor_create_count"] = 1 if action == "create" else 0
        state.timing["executor_rebuild_count"] = 1 if action == "rebuild" else 0
        state.timing["pool_reuse_count"] = 1 if action == "reuse" else 0

    def _prepare_shared_pool_for_job(
        self,
        *,
        job_id: str,
        job_id_snapshot: str,
        artifact_key: str,
        requested_mode: str,
        reset_pool: bool,
        create_pool: Any,
    ) -> TaskPool:
        with self._lock:
            shared_before = self._shared_pool.executor if self._shared_pool is not None else None
        pool_prepare_started = time.monotonic()
        executor = self._get_or_create_shared_pool(
            artifact_key=artifact_key,
            requested_mode=requested_mode,
            reset_pool=reset_pool,
            create_pool=create_pool,
        )
        pool_prepare_ms = _ms(time.monotonic() - pool_prepare_started)
        executor._job_id = job_id_snapshot  # noqa: SLF001
        action = "reuse" if shared_before is not None and shared_before is executor else ("rebuild" if shared_before is not None else "create")
        with self._lock:
            self._current_executor = executor
            self._record_pool_prepare_timing_locked(
                state=self._jobs.get(job_id),
                action=action,
                pool_prepare_ms=pool_prepare_ms,
            )
        return executor

    def _fanout_job_update_globals(
        self,
        *,
        job_id: str,
        job_id_snapshot: str,
        executor: Any,
        prepared_update_globals: Dict[str, object],
        phase_log: bool,
    ) -> float:
        if not prepared_update_globals:
            return 0.0
        warmup_started = time.monotonic()
        with self._lock:
            current_state = self._jobs.get(job_id)
            if current_state is not None:
                current_state.checkpoint["phase"] = "fanout_globals"
        if phase_log:
            logger.info(
                "[JobQueue] phase=fanout_globals job_id=%s key_count=%d",
                job_id_snapshot,
                len(prepared_update_globals),
            )
        else:
            logger.info(
                "[JobQueue] update_globals job_id=%s key_count=%d",
                job_id_snapshot,
                len(prepared_update_globals),
            )
        executor.update_globals(dict(prepared_update_globals))
        warmup_ms = _ms(time.monotonic() - warmup_started)
        with self._lock:
            current_state = self._jobs.get(job_id)
            if current_state is not None:
                current_state.timing["warmup_ms"] = warmup_ms
                current_state.timing["fanout_globals_ms"] = warmup_ms
        if phase_log:
            logger.info(
                "[JobQueue] phase=fanout_globals_done job_id=%s key_count=%d",
                job_id_snapshot,
                len(prepared_update_globals),
            )
        return warmup_ms

    def _build_hook_job_task_pool_spec(
        self,
        *,
        payload: Dict[str, object],
        client_id: str,
        job_id_snapshot: str,
        pool_request: _JobSharedPoolRequest,
        blob: bytes,
        package_format: str,
        entry_module: str,
        task_entry_callable: str,
        task_resource_paths: Sequence[str],
        effective_managed_global_names: Sequence[str],
    ) -> _JobTaskPoolSpec:
        artifact_key = self._shared_pool_artifact_key(
            blob=blob,
            runtime=str(payload.get("runtime", "py3") or "py3"),
            entry_module=entry_module,
            entry_callable=task_entry_callable,
            package_format=package_format,
            dependency_allowlist=list(payload.get("dependency_allowlist") or ()),
            managed_global_names=effective_managed_global_names,
            task_resource_paths=task_resource_paths,
        )

        def _create_pool(mode: str) -> TaskPool:
            dependency_allowlist = list(payload.get("dependency_allowlist") or ())
            task_pool_kwargs = {
                "target": self._controlplane_target,
                "job_id": job_id_snapshot,
                "owner_client_id": client_id,
                "pool_name": str(payload.get("pool_name", "") or f"job-pool-{job_id_snapshot}"),
                "runtime": str(payload.get("runtime", "py3") or "py3"),
                "serialization_mode": pool_request.raw_requested_mode or "",
                "policy_id": self._taskpool_policy_id,
                "deps": ArtifactDeps.allow_install(dependency_allowlist) if dependency_allowlist else None,
                "managed_global_names": list(effective_managed_global_names or ()),
                "worker_count": max(1, int(payload.get("pool_worker_count", payload.get("worker_count", pool_request.default_worker_count)) or pool_request.default_worker_count)),
                "heartbeat_timeout_sec": max(5, int(payload.get("pool_heartbeat_timeout_sec", 30) or 30)),
                "idle_ttl_sec": max(0, int(payload.get("pool_idle_ttl_sec", 0) or 0)),
                "healthy_only": bool(payload.get("healthy_only", True)),
                "tags": list(payload.get("tags") or ()),
                "node_ids": list(payload.get("node_ids") or ()),
                "node_count": max(0, int(payload.get("pool_node_count", payload.get("node_count", pool_request.default_node_count) or pool_request.default_node_count) or 0)),
                "node_limit": int(payload.get("node_limit", 100) or 100),
                "timeout_sec": float(payload.get("timeout_sec", 10.0) or 10.0),
            }
            if task_resource_paths:
                task_pool_kwargs["resource_paths"] = list(task_resource_paths)
            task_pool_kwargs.update(
                source=blob,
                entry_module=entry_module,
                entry_callable=task_entry_callable,
                package_format=package_format,
            )
            return _create_job_task_pool(**task_pool_kwargs)

        return _JobTaskPoolSpec(artifact_key=artifact_key, create_pool=_create_pool)

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
        if any(str(normalized_payload.get(field, "") or "").strip() for field in ("policy_id", "taskpool_policy_id")):
            raise ValueError(
                "job submit policy_id/taskpool_policy_id is not supported; policy is owned by startup node/deployment"
            )
        if not _uses_job_hooks(normalized_payload):
            raise ValueError(
                "job submit requires hook mode; use JobQueue.submit(source=...) "
                "or provide job_mode='hooks' with task_generator_callable"
            )
        _validate_delayed_resolve_refs(normalized_payload)
        job_id = str(normalized_payload.get("job_id", "") or "").strip() or f"jobq-{uuid.uuid4().hex}"
        client_id = str(normalized_payload.get("client_id", "") or "").strip() or f"job-client-{uuid.uuid4().hex[:8]}"
        priority = max(0, int(normalized_payload.get("priority", 0) or 0))
        owner_token_digest = _auth_token_digest(auth_token)
        submitted_at = utc_now()
        owner_token_expires_at = submitted_at + timedelta(seconds=_job_auth_ttl_sec()) if owner_token_digest else None
        staged_ref_ids = _collect_payload_data_ref_ids(normalized_payload)
        executor_to_close = None
        with self._cv:
            executor_to_close = self._detach_stale_running_locked()
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
            state.error_preview = ""
            state.final_result_preview = ""
            self._jobs[job_id] = state
            state._timing_marks["submitted_at_monotonic"] = time.monotonic()
            self._insert_waiting_job_locked(state)
            self._invalidate_summary_locked()
            self._ensure_scheduler_thread_locked()
            self._cv.notify_all()
        logger.info(
            "[JobQueue] submit job_id=%s client_id=%s status=%s entry_module=%s entry_callable=%s job_mode=%s task_mode=%s reset_pool=%s priority=%s",
            job_id,
            client_id,
            state.status,
            state.entry_module,
            state.entry_callable,
            str(normalized_payload.get("job_mode", "") or ""),
            str(normalized_payload.get("task_serialization_mode", "") or ""),
            bool(normalized_payload.get("reset_pool", False)),
            priority,
        )
        self._submit_executor_close(executor_to_close)
        return state

    def get_job(self, job_id: str) -> Optional[JobState]:
        with self._lock:
            executor_to_close = self._detach_stale_running_locked()
            if any(item.status == "WAITING" and not item.cancel_requested for item in self._jobs.values()):
                self._ensure_scheduler_thread_locked()
            state = self._jobs.get(str(job_id or "").strip())
        self._submit_executor_close(executor_to_close)
        return state

    def summary(self, *, recent_limit: int = 5, waiting_limit: int = 50) -> Dict[str, object]:
        with self._lock:
            executor_to_close = self._detach_stale_running_locked()
            if any(item.status == "WAITING" and not item.cancel_requested for item in self._jobs.values()):
                self._ensure_scheduler_thread_locked()
            if (
                self._summary_cache is not None
                and (time.monotonic() - self._summary_cache_built_at_monotonic) <= self._summary_cache_ttl_sec
            ):
                cached = dict(self._summary_cache)
                self._submit_executor_close(executor_to_close)
                return cached
            jobs = list(self._jobs.values())
            current = str(self._running_job_id or "").strip()
            current_state = self._jobs.get(current) if current else None
            waiting_order = list(self._waiting_order)
            current_job_status = str(current_state.status) if current_state is not None else ""
            current_job_phase = str(((current_state.checkpoint or {}).get("phase", "") if current_state is not None else "") or "")
            current_job_timing = dict(current_state.timing or {}) if current_state is not None else {}
            aggregate_timing = self._aggregate_job_timing_locked(jobs)
            cache_revision = self._summary_cache_revision
        self._submit_executor_close(executor_to_close)
        waiting = sum(1 for item in jobs if item.status == "WAITING" and not item.cancel_requested)
        running = sum(1 for item in jobs if item.status == "RUNNING")
        succeeded = sum(1 for item in jobs if item.status == "SUCCEEDED")
        failed = sum(1 for item in jobs if item.status == "FAILED")
        cancelled = sum(1 for item in jobs if item.status == "CANCELLED")
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
        job_map = {item.job_id: item for item in jobs}
        for position, job_id in enumerate(waiting_order[: max(0, int(waiting_limit or 0))], start=1):
            item = job_map.get(job_id)
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
        summary_payload = {
            "job_count": len(jobs),
            "waiting": waiting,
            "running": running,
            "succeeded": succeeded,
            "failed": failed,
            "cancelled": cancelled,
            "terminal": succeeded + failed + cancelled,
            "current_job_id": current,
            "current_job_status": current_job_status,
            "current_job_phase": current_job_phase,
            "current_job_timing": current_job_timing,
            "recent_jobs": [
                {
                    "job_id": str(item.job_id or ""),
                    "status": str(item.status or ""),
                    "submitted_at": item.submitted_at.isoformat(),
                    "finished_at": item.finished_at.isoformat() if item.finished_at else "",
                    "final_result_preview": str(item.final_result_preview or ""),
                    "error_preview": str(item.error_preview or ""),
                    "timing": dict(item.timing or {}),
                }
                for item in recent_jobs
            ],
            "waiting_jobs": waiting_jobs,
            "timing": aggregate_timing,
        }
        with self._lock:
            if cache_revision == self._summary_cache_revision:
                self._summary_cache = dict(summary_payload)
                self._summary_cache_built_at_monotonic = time.monotonic()
        return summary_payload

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
                self._refresh_job_previews_locked(normalized)
                self._remove_waiting_job_locked(state.job_id)
                self._invalidate_summary_locked()
                release_state = JobState(**{**state.__dict__})
            elif state.status == "RUNNING":
                state.cancel_requested = True
                self._invalidate_summary_locked()
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
            self._invalidate_summary_locked()
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
        if released:
            self._invalidate_summary_locked()
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

    def _job_ref_snapshot(self, state: Optional[JobState]) -> tuple[str, List[str]]:
        if state is None:
            return "", []
        return (
            str(state.job_id or "").strip(),
            [str(ref_id).strip() for ref_id in list(state.staged_ref_ids or ()) if str(ref_id).strip()],
        )

    def _touch_refs_for_job(
        self,
        *,
        job_id: str,
        ref_ids: Sequence[str],
        invalidate_summary: bool,
    ) -> None:
        snapshot = [str(ref_id).strip() for ref_id in list(ref_ids or ()) if str(ref_id).strip()]
        if not self._controlplane_target or not snapshot:
            return
        client = DataRegistryClient(self._controlplane_target, timeout_sec=5.0)
        touched = False
        for ref_id in snapshot:
            try:
                client.touch(ref_id)
                touched = True
            except Exception:
                continue
        if touched:
            with self._lock:
                current = self._jobs.get(str(job_id or "").strip())
                if current is not None:
                    current.last_ref_touch_at = utc_now()
                    if invalidate_summary:
                        self._invalidate_summary_locked()

    def _release_refs_for_job(
        self,
        *,
        job_id: str,
        ref_ids: Sequence[str],
        invalidate_summary: bool,
    ) -> None:
        snapshot = [str(ref_id).strip() for ref_id in list(ref_ids or ()) if str(ref_id).strip()]
        if not self._controlplane_target or not snapshot:
            return
        client = DataRegistryClient(self._controlplane_target, timeout_sec=5.0)
        for ref_id in snapshot:
            with contextlib.suppress(Exception):
                client.release(ref_id)
        with self._lock:
            current = self._jobs.get(str(job_id or "").strip())
            if current is not None:
                current.staged_ref_ids = [ref_id for ref_id in current.staged_ref_ids if ref_id not in snapshot]
                if invalidate_summary:
                    self._invalidate_summary_locked()

    def _touch_job_refs_snapshot(self, *, job_id: str, ref_ids: Sequence[str]) -> None:
        self._touch_refs_for_job(job_id=job_id, ref_ids=ref_ids, invalidate_summary=False)

    def _release_job_refs(self, state: JobState) -> None:
        job_id, ref_ids = self._job_ref_snapshot(state)
        self._release_refs_for_job(job_id=job_id, ref_ids=ref_ids, invalidate_summary=True)

    def _release_job_refs_snapshot(self, *, job_id: str, ref_ids: Sequence[str]) -> None:
        self._release_refs_for_job(job_id=job_id, ref_ids=ref_ids, invalidate_summary=False)

    def _loop(self) -> None:
        while True:
            refs_to_touch: List[tuple[str, List[str]]] = []
            refs_to_release: List[tuple[str, List[str]]] = []
            next_job: Optional[JobState] = None
            shared_pool_to_close: Any = None
            with self._cv:
                while not self._stop and self._running_job_id:
                    current_state = self._jobs.get(self._running_job_id)
                    if current_state is None or current_state.status in {"SUCCEEDED", "FAILED", "CANCELLED"}:
                        if self._shared_pool is not None:
                            self._shared_pool.last_used_at = utc_now()
                        self._running_job_id = ""
                        self._current_executor = None
                        self._invalidate_summary_locked()
                        self._cv.notify_all()
                        break
                    self._cv.wait(timeout=0.1)
                if self._stop:
                    return
                refs_to_release = [
                    (state.job_id, list(state.staged_ref_ids))
                    for state in self._cleanup_jobs_locked()
                    if state.staged_ref_ids
                ]
                refs_to_touch = [
                    (state.job_id, list(state.staged_ref_ids))
                    for state in self._jobs_needing_ref_touch_locked(now=utc_now())
                    if state.staged_ref_ids
                ]
                next_job = self._pick_next_job_locked()
                if next_job is None:
                    if not self._running_job_id and self._shared_pool_idle_expired_locked(now=utc_now()):
                        if self._shared_pool is not None:
                            logger.info(
                                "[JobQueue] shared pool idle expiry artifact_key=%s idle_ttl_sec=%s",
                                _artifact_key_preview(self._shared_pool.artifact_key),
                                self._pool_idle_ttl_sec,
                            )
                        shared_pool_to_close = self._close_shared_pool_locked()
                    self._cv.wait(timeout=0.1)
                else:
                    next_job.status = "RUNNING"
                    next_job.started_at = utc_now()
                    if next_job.checkpoint is None:
                        next_job.checkpoint = {}
                    next_job.checkpoint["phase"] = "preparing"
                    submitted_mark = next_job._timing_marks.get("submitted_at_monotonic")
                    if submitted_mark is not None:
                        next_job.timing["queue_wait_ms"] = _ms(time.monotonic() - submitted_mark)
                    self._remove_waiting_job_locked(next_job.job_id)
                    self._running_job_id = next_job.job_id
                    self._invalidate_summary_locked()
                    logger.info(
                        "[JobQueue] start job_id=%s client_id=%s entry_module=%s job_mode=%s",
                        next_job.job_id,
                        next_job.client_id,
                        next_job.entry_module,
                        str(next_job.payload.get("job_mode", "") or ""),
                    )
            for job_id, ref_ids in refs_to_release:
                self._maintenance_executor.submit(self._release_job_refs_snapshot, job_id=job_id, ref_ids=ref_ids)
            for job_id, ref_ids in refs_to_touch:
                self._maintenance_executor.submit(self._touch_job_refs_snapshot, job_id=job_id, ref_ids=ref_ids)
            self._submit_executor_close(shared_pool_to_close)
            if next_job is None:
                continue
            self._run_job(next_job.job_id)
            terminal_state = self.get_job(next_job.job_id)
            if terminal_state is not None and terminal_state.status in {"SUCCEEDED", "FAILED", "CANCELLED"}:
                logger.info(
                    "[JobQueue] terminal job_id=%s status=%s results=%d error=%s",
                    terminal_state.job_id,
                    terminal_state.status,
                    len(list(terminal_state.results or ())),
                    _preview_job_value(terminal_state.error),
                )
                self._release_job_refs(terminal_state)
            with self._cv:
                if self._shared_pool is not None:
                    self._shared_pool.last_used_at = utc_now()
                self._running_job_id = ""
                self._current_executor = None
                self._invalidate_summary_locked()
                self._cv.notify_all()

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
                    self._job_timing_mark_locked(job_id, "resolve_refs")
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
                    self._job_timing_finish_locked(job_id, "resolve_refs", "resolve_refs_ms")
                    state.checkpoint["phase"] = "selecting_nodes"
        except Exception as exc:
            with self._lock:
                state = self._jobs.get(job_id)
                if state is not None:
                    self._job_timing_finish_locked(job_id, "resolve_refs", "resolve_refs_ms")
                    self._job_timing_finalize_locked(job_id)
                state.status = "FAILED"
                state.finished_at = utc_now()
                state.error = str(exc)
                self._invalidate_summary_locked()
            return

        if _uses_job_hooks(payload):
            self._run_job_with_hooks(
                job_id=job_id,
                payload=payload,
                client_id=client_id,
                job_id_snapshot=job_id_snapshot,
            )
            return
        with self._lock:
            state = self._jobs.get(job_id)
            if state is not None:
                self._job_timing_finalize_locked(job_id)
                state.status = "FAILED"
                state.finished_at = utc_now()
                state.error = "job submit requires hook mode"
                self._refresh_job_previews_locked(job_id)

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
                    self._invalidate_summary_locked()
            return

        package_format = str(payload.get("package_format", "py") or "py").strip() or "py"
        entry_module = str(payload.get("entry_module", "") or "").strip()
        task_entry_callable = str(payload.get("entry_callable", "run") or "run").strip() or "run"
        raw_task_generator = payload.get("task_generator_callable", "task_generator")
        task_resource_paths = [str(item).strip() for item in list(payload.get("task_resource_paths") or ()) if str(item).strip()]
        pool_request = self._resolve_job_shared_pool_request(payload)
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
            logger.info(
                "[JobQueue] run job_id=%s mode=hooks entry_module=%s requested_mode=%s reset_pool=%s",
                job_id_snapshot,
                entry_module,
                pool_request.requested_mode,
                pool_request.reset_pool,
            )
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
            with self._lock:
                self._job_timing_mark_locked(job_id, "select_nodes")
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
            with self._lock:
                self._job_timing_finish_locked(job_id, "select_nodes", "select_nodes_ms")

            pool_spec = self._build_hook_job_task_pool_spec(
                payload=payload,
                client_id=client_id,
                job_id_snapshot=job_id_snapshot,
                pool_request=pool_request,
                blob=blob,
                package_format=package_format,
                entry_module=entry_module,
                task_entry_callable=task_entry_callable,
                task_resource_paths=task_resource_paths,
                effective_managed_global_names=effective_managed_global_names,
            )
            executor = self._prepare_shared_pool_for_job(
                job_id=job_id,
                job_id_snapshot=job_id_snapshot,
                artifact_key=pool_spec.artifact_key,
                requested_mode=pool_request.requested_mode,
                reset_pool=pool_request.reset_pool,
                create_pool=pool_spec.create_pool,
            )
            self._fanout_job_update_globals(
                job_id=job_id,
                job_id_snapshot=job_id_snapshot,
                executor=executor,
                prepared_update_globals=prepared_update_globals,
                phase_log=True,
            )

            produced = _resolve_task_generator_output(
                task_generator,
                module=module,
                payload=hook_kwargs,
            )
            if isinstance(produced, list):
                logger.info("[JobQueue] task_generator job_id=%s produced=%d", job_id_snapshot, len(produced))
            else:
                logger.info("[JobQueue] task_generator job_id=%s produced_type=%s", job_id_snapshot, type(produced).__name__)

            def _payload_stream() -> Any:
                for item in produced:
                    if not isinstance(item, dict):
                        raise RuntimeError("task_generator must yield dict payloads")
                    yield dict(item)

            state_obj: object = payload.get("initial_state")
            if state_obj is None:
                state_obj = {"results": []}
            if isinstance(produced, list):
                with self._lock:
                    current_state = self._jobs.get(job_id)
                    if current_state is not None:
                        current_state.timing["task_count"] = len(produced)
            rendered_results: List[Dict[str, object]] = []
            with self._lock:
                current_state = self._jobs.get(job_id)
                if current_state is not None:
                    current_state.checkpoint["phase"] = "running_tasks"
            logger.info(
                "[JobQueue] phase=running_tasks job_id=%s max_in_flight=%s receive_batch=%s",
                job_id_snapshot,
                _resolve_job_executor_max_in_flight(executor, payload.get("max_in_flight")),
                max(1, int(payload.get("receive_batch", 10) or 10)),
            )
            running_started = time.monotonic()
            first_result_wait_ms = 0.0
            stream = executor.imap_unordered(
                _payload_stream(),
                max_in_flight=_resolve_job_executor_max_in_flight(executor, payload.get("max_in_flight")),
                receive_batch=max(1, int(payload.get("receive_batch", 10) or 10)),
                submit_timeout_sec=float(payload.get("submit_timeout_sec", 60.0) or 60.0),
                result_timeout_sec=float(payload.get("result_timeout_sec", payload.get("wait_chunk_timeout_sec", 30.0)) or 30.0),
                wait_ms=int(payload.get("wait_ms", 500) or 500),
                raise_on_error=True,
                node_window_factor=float(payload.get("node_window_factor", 2.0) or 2.0),
                max_infra_retries=max(0, int(payload.get("max_infra_retries", payload.get("task_max_infra_retries", 1)) or 0)),
                retry_backoff_ms=max(0, int(payload.get("retry_backoff_ms", payload.get("task_retry_backoff_ms", 0)) or 0)),
                return_items=False,
            )
            for task_index, result in stream:
                result_index = int(task_index)
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
                        "index": result_index,
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
                            results_bucket.append({"index": result_index, "result": result})
                else:
                    returned_state = handle_result(
                        result_index,
                        result,
                        state=state_obj,
                        job_payload=dict(hook_kwargs),
                        job_id=job_id_snapshot,
                        client_id=client_id,
                    )
                    if returned_state is not None:
                        state_obj = returned_state
                if first_result_wait_ms <= 0.0:
                    first_result_wait_ms = _ms(time.monotonic() - running_started)
            logger.info(
                "[JobQueue] phase=running_tasks_done job_id=%s count=%d",
                job_id_snapshot,
                len(rendered_results),
            )
            running_tasks_ms = _ms(time.monotonic() - running_started)
            with self._lock:
                current_state = self._jobs.get(job_id)
                if current_state is not None:
                    current_state.timing["result_count"] = len(rendered_results)
                    current_state.timing["first_result_wait_ms"] = first_result_wait_ms
                    current_state.timing["running_tasks_ms"] = running_tasks_ms

            final_result = state_obj
            if finalize is not None:
                finalize_started = time.monotonic()
                with self._lock:
                    current_state = self._jobs.get(job_id)
                    if current_state is not None:
                        current_state.checkpoint["phase"] = "finalize"
                logger.info(
                    "[JobQueue] phase=finalize job_id=%s callable=%s",
                    job_id_snapshot,
                    finalize_callable or getattr(finalize, "__name__", "finalize"),
                )
                finalized = finalize(
                    state=state_obj,
                    job_payload=dict(hook_kwargs),
                    job_id=job_id_snapshot,
                    client_id=client_id,
                )
                if finalized is not None:
                    final_result = finalized
                finalize_ms = _ms(time.monotonic() - finalize_started)
                with self._lock:
                    current_state = self._jobs.get(job_id)
                    if current_state is not None:
                        current_state.timing["finalize_ms"] = finalize_ms
                logger.info(
                    "[JobQueue] phase=finalize_done job_id=%s final_type=%s",
                    job_id_snapshot,
                    type(final_result).__name__,
                )

            with self._lock:
                current_state = self._jobs.get(job_id)
                if current_state is not None:
                    current_state.checkpoint["phase"] = "terminal_writeback"
            logger.info(
                "[JobQueue] phase=terminal_writeback job_id=%s results=%d final_type=%s",
                job_id_snapshot,
                len(rendered_results),
                type(final_result).__name__,
            )
            terminal_writeback_started = time.monotonic()
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
                state.checkpoint["phase"] = "terminal_done"
                state.timing["terminal_writeback_ms"] = _ms(time.monotonic() - terminal_writeback_started)
                self._refresh_job_previews_locked(job_id)
                self._job_timing_finalize_locked(job_id)
            logger.info(
                "[JobQueue] phase=terminal_done job_id=%s status=%s results=%d",
                job_id_snapshot,
                "SUCCEEDED" if not state.cancel_requested else "CANCELLED",
                len(rendered_results),
            )
        except Exception as exc:
            logger.exception("[JobQueue] run hooks job failed job_id=%s", job_id_snapshot)
            with self._lock:
                state = self._jobs.get(job_id)
                if state is not None:
                    self._job_timing_finalize_locked(job_id)
                    state.status = "FAILED"
                    state.finished_at = utc_now()
                    state.error = str(exc)
                    self._refresh_job_previews_locked(job_id)
        finally:
            stream = None
            produced = None
            finalize = None
            handle_result = None
            task_generator = None
            extracted_dir = str(getattr(module, "__pycloud_temp_extract_dir__", "") or "").strip() if module is not None else ""
            task_entry = None
            module = None
            try:
                _purge_loaded_artifact_modules(
                    str(tmp_path),
                    entry_module=entry_module,
                    package_format=package_format,
                    dependency_path="",
                    extra_prefixes=([extracted_dir] if extracted_dir else []),
                )
            except Exception:
                pass
            tmp_path.unlink(missing_ok=True)
            if extracted_dir:
                shutil.rmtree(extracted_dir, ignore_errors=True)
            gc.collect()
