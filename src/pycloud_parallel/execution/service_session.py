from __future__ import annotations

"""Authoritative V1 service execution implementation."""

from dataclasses import InitVar, dataclass, field
from datetime import datetime, timezone, timedelta
import asyncio
import contextlib
from concurrent.futures import ThreadPoolExecutor, as_completed
import importlib
import inspect
import io
import json
import logging
import math
import os
from pathlib import Path
import signal
import threading
import time
from typing import Any, AsyncIterator, Callable, Dict, Iterator, List, Optional, Sequence, Set, Tuple, Union
import uuid
from urllib.parse import urlparse

from pycloud_parallel.controlplane.artifact import (
    Artifact,
    ArtifactDeps,
    ArtifactExports,
    _default_artifact_filename,
    _default_entry_module_for_func,
    _default_entry_module_for_module,
    _normalize_artifact_input,
    _normalize_entry_callable_arg,
    _normalize_entry_module_arg,
    _prepare_artifact,
    _resolve_package_format,
)
from pycloud_parallel.controlplane.config import OBJECT_CHUNK_SIZE_BYTES
from pycloud_parallel.controlplane.effective_policy import (
    EffectivePolicy,
    resolve_effective_policy,
)
from pycloud_parallel.controlplane.infocenter_client import (
    InfoCenterNode,
    InfoCenterServiceRoute,
    NodeCircuitState,
    _build_unique_node_id_map,
    _node_instance_key_from_node,
    _node_instance_key_from_route,
    _route_sort_key,
)
from pycloud_parallel.controlplane.node_control_transport import (
    new_node_control_client as _new_node_control_client,
    node_control_client as _node_control_client,
    node_control_target_for_node as _node_control_target_for_node,
)
from pycloud_parallel.controlplane.policy_profile import (
    get_default_policy_id_for_binding,
    get_policy_profile,
)
from pycloud_parallel.controlplane.session_model import SessionBinding, SessionIdentity
from pycloud_parallel.controlplane.scheduling_policy import is_admitted_node, node_admission_block_reason
from pycloud_parallel.data.ref import DataRef
from pycloud_parallel.controlplane.replica_client import ServiceSessionClient
from pycloud_parallel.controlplane.session_handle import ExecutionReplicaHandle
from pycloud_parallel.controlplane.serialization import (
    LOCAL_IPC_SERIALIZATION_MODE,
    decode_inline_transport_carrier,
    is_inline_transport_carrier,
)
from pycloud_parallel.controlplane.serialization_mode import resolve_effective_serialization_mode
from pycloud_parallel.controlplane.runtime_spec import matches_python_runtime, normalize_python_runtime_spec
from pycloud_parallel.execution.failover import (
    CandidateBreakerState,
    CONTROLPLANE_UNAVAILABLE,
    ROUTE_UNAVAILABLE,
    STAGING_FAILED,
    before_probe,
    candidate_allowed,
    classify_service_error,
    mark_candidate_failure,
    mark_candidate_success,
    should_failover,
)
from pycloud_parallel.execution.managed_globals import update_managed_globals_across_replicas
from pycloud_parallel.execution.progress import ProgressOption, ProgressReporter
from pycloud_parallel.execution.deployment_create_helper import (
    dispatch_create_requests,
    normalize_initial_globals,
    prepare_deployment_artifact,
)
from pycloud_parallel.execution.base import ExecutionItem, ServiceExecutionSession
from pycloud_parallel.execution.call_proxy import _BroadcastProxy, _CallProxy
from pycloud_parallel.execution.scheduler import (
    SERVICE_DEFAULT,
    SchedulerCandidate,
    SchedulerState,
    select_one_candidate,
    resolve_service_strategy,
)
from pycloud_parallel.execution.support import (
    _DEFAULT_EXPORT_DECORATOR,
    _RetryableReadyError,
    _SERVICE_SESSION_LOCKED_PATHS,
    _SERVICE_SESSION_LOCK_GUARD,
    _SERVICE_SESSION_SCHEMA_VERSION,
    _artifact_code_version,
    _default_service_session_cache_dir,
    _emit_owner_notice,
    _ensure_private_dir,
    _filter_nodes_by_runtime,
    _get_local_ip,
    _prepare_code_blob,
    _put_data_via_clients,
    _resolve_public_target_arg,
    _resolve_high_level_service_data,
    _resolve_high_level_service_results,
    _retry_infocenter_request,
    _sanitize_session_cache_part,
    _summarize_discovered_nodes,
    _timestamp_to_datetime,
    _write_private_json,
)
from pycloud_parallel.proto.v1 import pycloud_v1_pb2 as pb2
from pycloud_parallel.runtime.compat import runtime_mismatch_message_for_nodes

logger = logging.getLogger(__name__)


def _resolve_owner_api_token(api_token: str = "") -> str:
    return str(api_token or os.getenv("PYCLOUD_API_TOKEN", "") or "").strip()

_STARTUP_PREFLIGHT_RETRY_SEC = 5.0
_STARTUP_PREFLIGHT_SLEEP_SEC = 0.2
_LOCAL_SERVICE_EXECUTOR_POLL_INTERVAL_SEC = 0.25
_CONNECTED_SERVICE_OWNER_ONLY_METHODS = frozenset({"update_globals"})


def _infocenter_client(*args, **kwargs):
    from pycloud_parallel.controlplane.infocenter_client import InfoCenterClient

    return InfoCenterClient(*args, **kwargs)


def _endpoint_from_url_or_addr(value: str) -> Tuple[str, int]:
    text = str(value or "").strip()
    if not text:
        return "", 0
    parsed = urlparse(text)
    if parsed.hostname and parsed.port:
        return parsed.hostname.strip().lower(), int(parsed.port)
    if ":" not in text:
        return "", 0
    host, port = text.rsplit(":", 1)
    try:
        return host.strip("[]").lower(), int(port)
    except ValueError:
        return "", 0


def _startup_expected_endpoint(*, service_http_base_url: str, service_http_bind: str) -> Tuple[str, int]:
    endpoint = _endpoint_from_url_or_addr(service_http_base_url)
    if endpoint[1] > 0:
        return endpoint
    return _endpoint_from_url_or_addr(service_http_bind)


def _route_endpoint(route: InfoCenterServiceRoute) -> Tuple[str, int]:
    endpoint = _endpoint_from_url_or_addr(str(getattr(route, "http_base_url", "") or ""))
    if endpoint[1] > 0:
        return endpoint
    return _endpoint_from_url_or_addr(str(getattr(route, "control_addr", "") or ""))


def _startup_endpoint_matches(left: Tuple[str, int], right: Tuple[str, int]) -> bool:
    left_host, left_port = left
    right_host, right_port = right
    if left_port <= 0 or right_port <= 0 or left_port != right_port:
        return False
    wildcard_hosts = {"", "0.0.0.0", "::", "[::]"}
    if left_host in wildcard_hosts or right_host in wildcard_hosts:
        return True
    return left_host == right_host


def _startup_active_routes(routes: Sequence[InfoCenterServiceRoute]) -> List[InfoCenterServiceRoute]:
    return [
        route
        for route in routes
        if bool(getattr(route, "node_healthy", True))
        and int(getattr(route, "status", 0) or 0)
        in {
            int(pb2.SERVICE_STATUS_STARTING),
            int(pb2.SERVICE_STATUS_RUNNING),
            int(pb2.SERVICE_STATUS_DRAINING),
        }
    ]


def _route_attr(route: object, name: str, default: object = "") -> object:
    if isinstance(route, dict):
        return route.get(name, default)
    return getattr(route, name, default)


def _route_summary_item(route: object) -> Dict[str, object]:
    return {
        "node_instance_id": str(_route_attr(route, "node_instance_id", "") or ""),
        "node_id": str(_route_attr(route, "node_id", "") or ""),
        "control_addr": str(_route_attr(route, "control_addr", "") or ""),
        "service_name": str(_route_attr(route, "service_name", "") or ""),
        "service_id": str(_route_attr(route, "service_id", "") or ""),
        "http_base_url": str(_route_attr(route, "http_base_url", "") or ""),
    }


def _format_route_summary(routes: Sequence[Dict[str, object]]) -> str:
    rows = []
    for item in routes:
        node_instance_id = str(item.get("node_instance_id", "") or "")
        node_id = str(item.get("node_id", "") or "")
        control_addr = str(item.get("control_addr", "") or "")
        service_id = str(item.get("service_id", "") or "")
        http_base_url = str(item.get("http_base_url", "") or "")
        node_label = node_id or node_instance_id or "-"
        rows.append(
            f"{node_label}/{node_instance_id or '-'}@{control_addr or '-'}"
            f"(service_id={service_id or '-'}, http={http_base_url or '-'})"
        )
    return "[" + ", ".join(rows) + "]"


@dataclass
class _ServiceSessionFileLock:
    path: Path
    _fp: Optional[io.BufferedRandom] = field(default=None, init=False, repr=False)

    def acquire(self) -> "_ServiceSessionFileLock":
        normalized = str(self.path.resolve())
        with _SERVICE_SESSION_LOCK_GUARD:
            if normalized in _SERVICE_SESSION_LOCKED_PATHS:
                raise RuntimeError(f"local deploy session already holds cache lock: {self.path}")
            _SERVICE_SESSION_LOCKED_PATHS.add(normalized)
        try:
            _ensure_private_dir(self.path.parent)
            fp = open(self.path, "a+b")
        except Exception:
            with _SERVICE_SESSION_LOCK_GUARD:
                _SERVICE_SESSION_LOCKED_PATHS.discard(normalized)
            raise
        try:
            try:
                os.chmod(self.path, 0o600)
            except OSError:
                pass
            if os.name == "nt":
                import msvcrt

                fp.seek(0)
                msvcrt.locking(fp.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(fp.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except Exception as exc:
            fp.close()
            with _SERVICE_SESSION_LOCK_GUARD:
                _SERVICE_SESSION_LOCKED_PATHS.discard(normalized)
            raise RuntimeError(
                f"another local deploy process already owns service session cache lock: {self.path}"
            ) from exc
        self._fp = fp
        return self

    def write_json(self, payload: Dict[str, object]) -> None:
        if self._fp is None:
            raise RuntimeError("service session lock is not acquired")
        data = json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True).encode("utf-8") + b"\n"
        self._fp.seek(0)
        self._fp.truncate()
        self._fp.write(data)
        self._fp.flush()
        try:
            os.fsync(self._fp.fileno())
        except OSError:
            pass

    def clear(self) -> None:
        if self._fp is None:
            return
        self._fp.seek(0)
        self._fp.truncate()
        self._fp.flush()
        try:
            os.fsync(self._fp.fileno())
        except OSError:
            pass

    def close(self) -> None:
        if self._fp is None:
            return
        normalized = str(self.path.resolve())
        try:
            if os.name == "nt":
                import msvcrt

                self._fp.seek(0)
                msvcrt.locking(self._fp.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(self._fp.fileno(), fcntl.LOCK_UN)
        finally:
            try:
                self._fp.close()
            finally:
                self._fp = None
                with _SERVICE_SESSION_LOCK_GUARD:
                    _SERVICE_SESSION_LOCKED_PATHS.discard(normalized)


def _service_session_cache_file(
    *,
    owner_client_id: str,
    service_name: str,
    cache_dir: str = "",
) -> Path:
    base_dir = Path(cache_dir).expanduser() if str(cache_dir).strip() else _default_service_session_cache_dir()
    return (
        base_dir
        / _sanitize_session_cache_part(owner_client_id)
        / f"{_sanitize_session_cache_part(service_name)}.json"
    )


def _load_service_session_cache(
    *,
    owner_client_id: str,
    service_name: str,
    cache_dir: str = "",
) -> Optional[Dict[str, object]]:
    path = _service_session_cache_file(
        owner_client_id=owner_client_id,
        service_name=service_name,
        cache_dir=cache_dir,
    )
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    if int(payload.get("schema_version", 0) or 0) != _SERVICE_SESSION_SCHEMA_VERSION:
        return None
    if payload.get("owner_client_id") != owner_client_id or payload.get("service_name") != service_name:
        return None
    nodes = payload.get("nodes")
    if not isinstance(nodes, dict):
        return None
    return payload


def _env_float(name: str, default: float) -> float:
    raw = str(os.environ.get(name, "") or "").strip()
    if not raw:
        return float(default)
    try:
        return float(raw)
    except ValueError:
        return float(default)


_SERVICE_READY_GRACE_SEC = max(0.0, _env_float("PYCLOUD_SERVICE_READY_GRACE_SEC", 5.0))
_SERVICE_READY_RETRY_INTERVAL_SEC = max(0.05, _env_float("PYCLOUD_SERVICE_READY_RETRY_INTERVAL_SEC", 0.25))
_SERVICE_SESSION_LOCK_RETRY_SEC = max(0.0, _env_float("PYCLOUD_SERVICE_SESSION_LOCK_RETRY_SEC", 3.0))
_DEFAULT_MAX_IN_FLIGHT_WORKER_FACTOR = 1.5


def _scaled_default_max_in_flight(total_workers: int) -> int:
    return max(1, int(math.ceil(float(max(1, int(total_workers or 0))) * _DEFAULT_MAX_IN_FLIGHT_WORKER_FACTOR)))


def _service_route_worker_count(route: object) -> int:
    if isinstance(route, dict):
        alive_workers = max(0, int(route.get("alive_workers", 0) or 0))
        worker_count = max(0, int(route.get("worker_count", 0) or 0))
    else:
        alive_workers = max(0, int(getattr(route, "alive_workers", 0) or 0))
        worker_count = max(0, int(getattr(route, "worker_count", 0) or 0))
    return max(alive_workers, worker_count, 0)


def _resolve_group_max_in_flight(group: object, *, max_in_flight: Optional[int], item_count: int) -> int:
    if max_in_flight is not None:
        try:
            normalized = int(max_in_flight)
        except Exception:
            normalized = 0
        if normalized > 0:
            return max(1, min(normalized, item_count))
    default_resolver = getattr(group, "_default_max_in_flight", None)
    if callable(default_resolver):
        try:
            resolved = int(default_resolver())
        except Exception:
            resolved = 1
    else:
        resolved = 1
    return max(1, min(resolved, item_count))


def _ready_retry_timeout(timeout_sec: float, *, grace_sec: float) -> float:
    effective_timeout = max(0.0, float(timeout_sec or 0.0))
    effective_grace = max(0.0, float(grace_sec or 0.0))
    if effective_timeout <= 0.0:
        return effective_grace
    if effective_grace <= 0.0:
        return 0.0
    return min(effective_timeout, effective_grace)


def _service_effective_policy_for_nodes(
    nodes: Sequence[InfoCenterNode | InfoCenterServiceRoute],
    *,
    policy_id: str = "",
    requested_mode: str = "",
    context: str = "",
) -> EffectivePolicy:
    del nodes
    return resolve_effective_policy(
        get_policy_profile(policy_id),
        requested_mode=requested_mode,
        context=context,
    )


def _candidate_policy_id(candidate: object) -> str:
    if isinstance(candidate, dict):
        return str(candidate.get("policy_id", "") or "").strip().lower()
    return str(getattr(candidate, "policy_id", "") or "").strip().lower()


def _resolve_bound_service_policy_id(
    candidates: Sequence[object],
    *,
    default_policy_id: str = "",
    context: str = "service",
) -> str:
    normalized_default = (
        str(default_policy_id or "").strip().lower()
        or get_default_policy_id_for_binding("service_internal")
    )
    discovered: List[str] = []
    missing = 0
    for candidate in candidates:
        policy_id = _candidate_policy_id(candidate)
        if policy_id:
            discovered.append(policy_id)
        else:
            missing += 1
    unique = sorted(set(discovered))
    if not unique:
        return normalized_default
    if len(unique) > 1:
        raise RuntimeError(f"{context} exposes inconsistent deploy-bound policy_id values: {unique}")
    if missing > 0:
        raise RuntimeError(
            f"{context} exposes mixed policy metadata: discovered={unique[0]!r} but {missing} route(s) are missing policy_id"
        )
    return unique[0]


def _retry_ready_state(
    fn: Callable[[], Any],
    *,
    timeout_sec: float,
    grace_sec: float,
    target: str,
    action: str,
    retry_interval_sec: float = _SERVICE_READY_RETRY_INTERVAL_SEC,
) -> Any:
    wait_timeout = _ready_retry_timeout(timeout_sec, grace_sec=grace_sec)
    if wait_timeout <= 0.0:
        return fn()
    deadline = time.monotonic() + wait_timeout
    last_exc: Optional[_RetryableReadyError] = None
    while True:
        try:
            return fn()
        except _RetryableReadyError as exc:
            last_exc = exc
            if time.monotonic() >= deadline:
                raise RuntimeError(
                    f"{action} not ready via {target} after {wait_timeout:.1f}s: {exc}"
                ) from exc
            time.sleep(min(max(0.05, float(retry_interval_sec or 0.25)), max(0.05, deadline - time.monotonic())))


def _acquire_service_session_lock_with_retry(
    path: Path,
    *,
    timeout_sec: float,
    action: str,
) -> _ServiceSessionFileLock:
    wait_timeout = _ready_retry_timeout(timeout_sec, grace_sec=_SERVICE_SESSION_LOCK_RETRY_SEC)
    deadline = time.monotonic() + wait_timeout
    last_exc: Optional[RuntimeError] = None
    while True:
        try:
            return _ServiceSessionFileLock(path).acquire()
        except RuntimeError as exc:
            last_exc = exc
            if wait_timeout <= 0.0 or time.monotonic() >= deadline:
                raise RuntimeError(f"{action}: {exc}") from exc
            time.sleep(min(_SERVICE_READY_RETRY_INTERVAL_SEC, max(0.05, deadline - time.monotonic())))


def _call_service_payload_sync(
    group: object,
    *,
    method: str,
    payload: Dict[str, object],
    timeout_sec: float,
    strategy: str,
    refresh_status: bool,
) -> Tuple[str, object]:
    node_id, response = group.call_balanced(
        method,
        payload,
        timeout_sec=timeout_sec,
        strategy=strategy,
        refresh_status=refresh_status,
    )
    return node_id, _resolve_high_level_service_data(group, node_id=node_id, response=response)


async def _call_service_payload_async(
    group: object,
    *,
    method: str,
    payload: Dict[str, object],
    timeout_sec: float,
    strategy: str,
    refresh_status: bool,
) -> Tuple[str, object]:
    node_id, response = await group.acall_balanced(
        method,
        payload,
        timeout_sec=timeout_sec,
        strategy=strategy,
        refresh_status=refresh_status,
    )
    return node_id, _resolve_high_level_service_data(group, node_id=node_id, response=response)


def _service_item_success(index: int, result: object, *, node_id: str) -> ExecutionItem:
    return ExecutionItem(
        index=int(index),
        ok=True,
        result=result,
        node_id=str(node_id or ""),
        key=int(index),
    )


def _service_item_failure(index: int, exc: Exception) -> ExecutionItem:
    return ExecutionItem(
        index=int(index),
        ok=False,
        result=None,
        error_type=exc.__class__.__name__,
        error_message=str(exc),
        node_id="",
        key=int(index),
    )


def _service_iter_item_calls(
    group: object,
    *,
    method: str,
    payloads: Sequence[Dict[str, object]],
    timeout_sec: float,
    strategy: str,
    refresh_status: bool,
    max_in_flight: Optional[int],
    progress: ProgressOption = False,
    progress_interval_sec: float = 2.0,
) -> Iterator[ExecutionItem]:
    try:
        item_count = len(payloads)  # type: ignore[arg-type]
    except Exception:
        item_count = 2**31 - 1
    limit = _resolve_group_max_in_flight(group, max_in_flight=max_in_flight, item_count=max(1, int(item_count or 1)))
    total = 0 if item_count == 2**31 - 1 else max(0, int(item_count or 0))

    def _generator() -> Iterator[ExecutionItem]:
        payload_iter = enumerate(
            payload if isinstance(payload, dict) else {}
            for payload in payloads
        )
        reporter = ProgressReporter(
            progress,
            label=f"service.{method}",
            total=total,
            interval_sec=progress_interval_sec,
        )
        submitted = 0
        completed = 0
        succeeded = 0
        failed = 0
        last_error = ""

        with ThreadPoolExecutor(max_workers=limit, thread_name_prefix="service-items") as executor:
            pending: Dict[object, int] = {}

            def _submit_next() -> bool:
                nonlocal submitted
                try:
                    idx, payload = next(payload_iter)
                except StopIteration:
                    return False
                future = executor.submit(
                    _call_service_payload_sync,
                    group,
                    method=method,
                    payload=payload,
                    timeout_sec=timeout_sec,
                    strategy=strategy,
                    refresh_status=refresh_status,
                )
                pending[future] = idx
                submitted += 1
                return True

            for _ in range(limit):
                if not _submit_next():
                    break
            reporter.emit(
                phase="running",
                completed=completed,
                succeeded=succeeded,
                failed=failed,
                inflight=len(pending),
                submitted=submitted,
                force=True,
            )

            while pending:
                for future in as_completed(tuple(pending.keys())):
                    idx = pending.pop(future)
                    break
                try:
                    node_id, result = future.result()
                    item = _service_item_success(idx, result, node_id=node_id)
                    succeeded += 1
                except Exception as exc:
                    item = _service_item_failure(idx, exc)
                    failed += 1
                    last_error = str(exc)
                completed += 1
                while len(pending) < limit and _submit_next():
                    pass
                reporter.emit(
                    phase="running",
                    completed=completed,
                    succeeded=succeeded,
                    failed=failed,
                    inflight=len(pending),
                    submitted=submitted,
                    last_error=last_error,
                )
                yield item
            reporter.done(completed=completed, succeeded=succeeded, failed=failed, submitted=submitted, last_error=last_error)

    return _generator()


async def _service_aiter_item_calls(
    group: object,
    *,
    method: str,
    payloads: Sequence[Dict[str, object]],
    timeout_sec: float,
    strategy: str,
    refresh_status: bool,
    max_in_flight: Optional[int],
    progress: ProgressOption = False,
    progress_interval_sec: float = 2.0,
) -> AsyncIterator[ExecutionItem]:
    items = [payload if isinstance(payload, dict) else {} for payload in payloads]
    if not items:
        return
    semaphore = asyncio.Semaphore(_resolve_group_max_in_flight(group, max_in_flight=max_in_flight, item_count=len(items)))
    reporter = ProgressReporter(
        progress,
        label=f"service.{method}",
        total=len(items),
        interval_sec=progress_interval_sec,
    )
    submitted = len(items)
    completed = 0
    succeeded = 0
    failed = 0
    last_error = ""

    async def _run_one(idx: int, payload: Dict[str, object]) -> ExecutionItem:
        async with semaphore:
            try:
                node_id, result = await _call_service_payload_async(
                    group,
                    method=method,
                    payload=payload,
                    timeout_sec=timeout_sec,
                    strategy=strategy,
                    refresh_status=refresh_status,
                )
                return _service_item_success(idx, result, node_id=node_id)
            except Exception as exc:
                return _service_item_failure(idx, exc)

    tasks = [asyncio.create_task(_run_one(idx, payload)) for idx, payload in enumerate(items)]
    reporter.emit(phase="running", completed=0, succeeded=0, failed=0, inflight=len(tasks), submitted=submitted, force=True)
    try:
        for task in asyncio.as_completed(tasks):
            item = await task
            completed += 1
            if item.ok:
                succeeded += 1
            else:
                failed += 1
                last_error = str(item.error_message or item.error_type or "")
            reporter.emit(
                phase="running",
                completed=completed,
                succeeded=succeeded,
                failed=failed,
                inflight=max(0, submitted - completed),
                submitted=submitted,
                last_error=last_error,
            )
            yield item
        reporter.done(completed=completed, succeeded=succeeded, failed=failed, submitted=submitted, last_error=last_error)
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()


def _service_collect_item_calls(
    group: object,
    *,
    method: str,
    payloads: Sequence[Dict[str, object]],
    timeout_sec: float,
    strategy: str,
    refresh_status: bool,
    max_in_flight: Optional[int],
    progress: ProgressOption = False,
    progress_interval_sec: float = 2.0,
) -> List[ExecutionItem]:
    return sorted(
        list(
            _service_iter_item_calls(
                group,
                method=method,
                payloads=payloads,
                timeout_sec=timeout_sec,
                strategy=strategy,
                refresh_status=refresh_status,
                max_in_flight=max_in_flight,
                progress=progress,
                progress_interval_sec=progress_interval_sec,
            )
        ),
        key=lambda item: int(item.index),
    )


async def _service_acollect_item_calls(
    group: object,
    *,
    method: str,
    payloads: Sequence[Dict[str, object]],
    timeout_sec: float,
    strategy: str,
    refresh_status: bool,
    max_in_flight: Optional[int],
    progress: ProgressOption = False,
    progress_interval_sec: float = 2.0,
) -> List[ExecutionItem]:
    items: List[ExecutionItem] = []
    async for item in _service_aiter_item_calls(
        group,
        method=method,
        payloads=payloads,
        timeout_sec=timeout_sec,
        strategy=strategy,
        refresh_status=refresh_status,
        max_in_flight=max_in_flight,
        progress=progress,
        progress_interval_sec=progress_interval_sec,
    ):
        items.append(item)
    return sorted(items, key=lambda item: int(item.index))


class _ConnectedService:
    """Unified product-facing connected service object for discovery/gateway routes."""

    def __init__(
        self,
        *,
        transport_client: Any,
        service_name: str,
        route: str,
        protocol: str = "http",
        timeout_sec: float,
        serialization_mode: str = "",
        validate_on_init: bool = True,
        effective_policy_override: Optional[EffectivePolicy] = None,
        prepare_discovery_payload: bool = True,
    ) -> None:
        self._transport_client = transport_client
        self.service_name = str(service_name or "").strip()
        self.route = str(route or "").strip().lower() or "discovery"
        self.protocol = str(protocol or "http").strip().lower() or "http"
        if self.protocol != "http":
            raise ValueError("Service.connect() protocol must be 'http'")
        self.timeout_sec = max(0.1, float(timeout_sec))
        self._requested_serialization_mode = str(serialization_mode or "").strip()
        self._fixed_effective_policy = effective_policy_override
        self._prepare_discovery_payload_enabled = bool(prepare_discovery_payload)
        self._default_policy_id = get_default_policy_id_for_binding(
            "gateway_public" if self.route == "gateway" else "service_internal"
        )
        self.target = str(
            getattr(transport_client, "target", "") or getattr(transport_client, "infocenter_target", "") or ""
        ).strip()
        self._route_cache = getattr(transport_client, "_route_cache", None)
        self._client_mod: Any = None
        if self.route == "discovery":
            from pycloud_parallel.controlplane import discovery_client as discovery_client_mod

            self._client_mod = discovery_client_mod.client_mod
        elif self.route == "gateway":
            from pycloud_parallel.controlplane import gateway_client as gateway_client_mod

            self._client_mod = gateway_client_mod.client_mod
        self._discovered_methods: Optional[List[str]] = None
        self._last_status: Optional[Dict[str, object]] = None
        self._route_notice_emitted = False
        self._async_call_gate: Optional[asyncio.Semaphore] = None
        self._async_call_gate_loop: Optional[asyncio.AbstractEventLoop] = None
        self._async_call_gate_capacity = 0
        self._async_call_executor: Optional[ThreadPoolExecutor] = None
        self._async_call_executor_capacity = 0
        self.effective_policy: Optional[EffectivePolicy] = None
        self.serialization_mode = str(serialization_mode or "").strip()
        if self.route == "local":
            self.effective_policy = None
            self._fixed_effective_policy = None
            self.serialization_mode = LOCAL_IPC_SERIALIZATION_MODE
        elif self._fixed_effective_policy is not None:
            self.effective_policy = self._fixed_effective_policy
            self._default_policy_id = self._fixed_effective_policy.policy_id
            self.serialization_mode = self._fixed_effective_policy.resolved_mode
        if not self.service_name:
            raise ValueError("Service.connect() requires service_name")
        if validate_on_init:
            self._validate_service_ready()
            self._refresh_effective_policy_from_routes()

    def close(self) -> None:
        if self._async_call_executor is not None:
            with contextlib.suppress(Exception):
                self._async_call_executor.shutdown(wait=False, cancel_futures=True)
            self._async_call_executor = None
            self._async_call_executor_capacity = 0
        close = getattr(self._transport_client, "close", None)
        if callable(close):
            close()

    def __enter__(self) -> "_ConnectedService":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def route_summary(self) -> List[Dict[str, object]]:
        if self.route == "discovery":
            routes = self._discoverable_routes(force_refresh=False)
        else:
            status = self.status()
            routes = list(status.get("routes", []) or []) if isinstance(status, dict) else []
        return [_route_summary_item(route) for route in routes]

    def routes(self) -> List[Dict[str, object]]:
        return self.route_summary()

    def _emit_route_notice_once(self, routes: Sequence[object]) -> None:
        if self._route_notice_emitted:
            return
        summary = [_route_summary_item(route) for route in routes]
        if not summary:
            return
        self._route_notice_emitted = True
        _emit_owner_notice(
            f"connected routes service_name={self.service_name} "
            f"route={self.route} protocol={self.protocol} routes={_format_route_summary(summary)}"
        )

    def _validate_service_ready(self) -> Dict[str, object]:
        def _probe() -> Dict[str, object]:
            if self.route == "discovery":
                try:
                    refresh = getattr(self._transport_client, "refresh_routes", None)
                    if callable(refresh):
                        refresh(service_name=self.service_name, force=True)
                    status = self._transport_client.get_status(service_name=self.service_name)
                except Exception as exc:
                    raise _RetryableReadyError(
                        f"failed to query {self.route} status for service_name={self.service_name!r}: {exc}"
                    ) from exc
            else:
                try:
                    status = self._transport_client.get_status(service_name=self.service_name)
                except Exception as exc:
                    raise _RetryableReadyError(
                        f"failed to query {self.route} status for service_name={self.service_name!r}: {exc}"
                    ) from exc
            if not isinstance(status, dict):
                raise RuntimeError(
                    f"invalid {self.route} status for service_name={self.service_name!r}: {status!r}"
                )
            self._last_status = status
            route_count = int(status.get("route_count", 0) or 0)
            if route_count <= 0:
                raise _RetryableReadyError(
                    f"Service.connect() could not find an available route for "
                    f"service_name={self.service_name!r} via {self.route}"
                )
            self._refresh_effective_policy_from_routes(status.get("routes", []) or [])
            self._emit_route_notice_once(status.get("routes", []) or [])
            return status

        return _retry_ready_state(
            _probe,
            timeout_sec=self.timeout_sec,
            grace_sec=_SERVICE_READY_GRACE_SEC,
            target=self.target or self.route,
            action=f"service connect {self.service_name!r}",
        )

    def _policy_context(self) -> str:
        return "gateway_public" if self.route == "gateway" else "service_connect"

    def _refresh_effective_policy_from_routes(self, routes: Optional[Sequence[object]] = None) -> None:
        if self._fixed_effective_policy is not None:
            self.effective_policy = self._fixed_effective_policy
            self._default_policy_id = self._fixed_effective_policy.policy_id
            self.serialization_mode = self._fixed_effective_policy.resolved_mode
            return
        candidates = list(routes or [])
        if not candidates:
            try:
                if self.route == "discovery":
                    candidates = list(self._discoverable_routes(force_refresh=False))
                elif isinstance(self._last_status, dict):
                    candidates = list(self._last_status.get("routes", []) or [])
            except Exception:
                candidates = []
        if not candidates:
            return
        bound_policy_id = _resolve_bound_service_policy_id(
            candidates,
            default_policy_id=self._default_policy_id,
            context=f"service_name={self.service_name!r}",
        )
        self.effective_policy = _service_effective_policy_for_nodes(
            candidates,
            policy_id=bound_policy_id,
            requested_mode=self._requested_serialization_mode,
            context=self._policy_context(),
        )
        self._default_policy_id = bound_policy_id
        self.serialization_mode = self.effective_policy.resolved_mode

    def _ensure_effective_policy_loaded(self, *, force_refresh: bool = False) -> None:
        if self.route == "discovery":
            routes = self._discoverable_routes(force_refresh=force_refresh)
        else:
            try:
                status = self._transport_client.get_status(service_name=self.service_name)
            except Exception:
                status = self._last_status or {}
            if isinstance(status, dict):
                self._last_status = status
                routes = list(status.get("routes", []) or [])
            else:
                routes = []
        self._refresh_effective_policy_from_routes(routes)

    def _ensure_methods_discovered(self) -> None:
        if self._discovered_methods is not None:
            return

        def _probe() -> List[str]:
            try:
                methods = self.list_methods(include_docs=True)
            except Exception as exc:
                self._validate_service_ready()
                raise _RetryableReadyError(
                    f"failed to list methods for service_name={self.service_name!r} via {self.route}: {exc}"
                ) from exc
            discovered = [str(item.get("method", "")).strip() for item in methods if str(item.get("method", "")).strip()]
            if not discovered:
                self._validate_service_ready()
                raise _RetryableReadyError(
                    f"service_name={self.service_name!r} has active {self.route} routes but no exported methods"
                )
            return discovered

        self._discovered_methods = _retry_ready_state(
            _probe,
            timeout_sec=self.timeout_sec,
            grace_sec=_SERVICE_READY_GRACE_SEC,
            target=self.target or self.route,
            action=f"service method discovery {self.service_name!r}",
        )

    def refresh_methods(self) -> List[str]:
        self._discovered_methods = None
        self._ensure_methods_discovered()
        return list(self._discovered_methods or [])

    def list_methods(self, *, include_docs: bool = False) -> List[Dict[str, object]]:
        if self.route == "discovery":
            route_cache = self._route_cache
            strategy = "predicted_busy"
            tried: Set[str] = set()
            try:
                if route_cache is not None:
                    route = route_cache.select_route(self.service_name, strategy=strategy)
                else:
                    routes = self._discoverable_routes(force_refresh=True)
                    if not routes:
                        raise RuntimeError(
                            f"Service.list_methods() could not find an available route for "
                            f"service_name={self.service_name!r}"
                        )
                    route = sorted(routes, key=lambda item: _route_sort_key(item, strategy=strategy))[0]
                tried.add(str(getattr(route, "service_id", "") or ""))
                methods = self._list_methods_via_route(route, include_docs=include_docs)
                if route_cache is not None:
                    with contextlib.suppress(Exception):
                        route_cache.mark_success(route)
            except Exception as exc:
                if route_cache is not None and "route" in locals():
                    with contextlib.suppress(Exception):
                        route_cache.mark_failure(route, str(exc))
                    with contextlib.suppress(Exception):
                        route_cache.refresh(self.service_name, force=True)
                    retry_route = route_cache.select_route(
                        self.service_name,
                        exclude_service_ids=tried,
                        strategy=strategy,
                    )
                else:
                    retry_candidates = [
                        item
                        for item in self._discoverable_routes(force_refresh=True)
                        if str(getattr(item, "service_id", "") or "") not in tried
                    ]
                    if not retry_candidates:
                        raise
                    retry_route = sorted(
                        retry_candidates,
                        key=lambda item: _route_sort_key(item, strategy=strategy),
                    )[0]
                methods = self._list_methods_via_route(retry_route, include_docs=include_docs)
                if route_cache is not None:
                    with contextlib.suppress(Exception):
                        route_cache.mark_success(retry_route)
        else:
            methods = self._transport_client.list_methods(
                service_name=self.service_name,
                include_docs=include_docs,
            )
        return list(methods)

    def _list_methods_via_route(self, route: object, *, include_docs: bool) -> List[Dict[str, object]]:
        if not str(getattr(route, "control_addr", "") or "").strip():
            return self._client_mod._list_route_methods_http(route, include_docs=include_docs, timeout_sec=self.timeout_sec)
        with _node_control_client(route.control_addr, timeout_sec=self.timeout_sec) as client:
            methods = client.list_service_methods(service_id=getattr(route, "service_id", ""), include_docs=include_docs)
        return [
            {
                "method": item.method,
                "qualified_name": item.qualified_name,
                "doc": item.doc,
            }
            for item in methods
        ]

    @property
    def methods(self) -> List[str]:
        self._ensure_methods_discovered()
        return list(self._discovered_methods or [])

    def status(self) -> Dict[str, object]:
        if self.route == "discovery":
            try:
                status = self._transport_client.get_status(service_name=self.service_name)
            except Exception:
                status = {}
            if isinstance(status, dict) and int(status.get("route_count", 0) or 0) > 0:
                self._last_status = status
                self._refresh_effective_policy_from_routes(status.get("routes", []) or [])
                return status
            routes = self._discoverable_routes()
            status = {
                "ok": True,
                "service_name": self.service_name,
                "route_count": len(routes),
                "routes": [self._client_mod._serialize_route(route) for route in routes],
            }
        else:
            status = self._transport_client.get_status(service_name=self.service_name)
        if isinstance(status, dict):
            self._last_status = status
            self._refresh_effective_policy_from_routes(status.get("routes", []) or [])
            self._emit_route_notice_once(status.get("routes", []) or [])
        return status

    def fetch_result_data(self, response_or_data: object, *, target_path: str = ""):
        return self._transport_client.fetch_result_data(response_or_data, target_path=target_path)

    def download_result_to_file(self, response_or_data: object, *, target_path: str):
        return self._transport_client.download_result_to_file(response_or_data, target_path=target_path)

    def _prepare_discovery_route_payload(self, route: object, payload: Dict[str, object]) -> Dict[str, object]:
        from pycloud_parallel.controlplane.remote_payload import (
            default_remote_call_object_threshold_bytes,
            prepare_remote_call_payload,
        )

        control_addr = str(getattr(route, "control_addr", "") or "").strip()
        if not control_addr:
            return payload if isinstance(payload, dict) else {}
        with _node_control_client(control_addr, timeout_sec=self.timeout_sec) as route_client:
            prepare_kwargs = {
                "object_threshold_bytes": default_remote_call_object_threshold_bytes(
                    effective_policy=self.effective_policy,
                ),
            }
            if str(self.serialization_mode or "").strip() and self.serialization_mode != "legacy_v1":
                prepare_kwargs["serialization_mode"] = self.serialization_mode
            return prepare_remote_call_payload(
                [route_client],
                payload,
                effective_policy=self.effective_policy,
                **prepare_kwargs,
            )

    def _discoverable_routes(self, *, force_refresh: bool = False) -> List[InfoCenterServiceRoute]:
        if self.route != "discovery":
            return []
        route_cache = self._route_cache
        routes: List[InfoCenterServiceRoute] = []
        if route_cache is not None:
            if force_refresh:
                with contextlib.suppress(Exception):
                    route_cache.refresh(self.service_name, force=True)
            with contextlib.suppress(Exception):
                routes = list(route_cache.get_routes(self.service_name))
        routes = [route for route in routes if str(getattr(route, "service_name", "") or "").strip() == self.service_name]
        if routes:
            self._refresh_effective_policy_from_routes(routes)
            self._emit_route_notice_once(routes)
            return routes
        routes = self._discover_routes_from_nodes()
        self._refresh_effective_policy_from_routes(routes)
        self._emit_route_notice_once(routes)
        return routes

    def _discover_routes_from_nodes(self) -> List[InfoCenterServiceRoute]:
        try:
            with _infocenter_client(self.target, timeout_sec=self.timeout_sec) as infocenter:
                nodes = list(
                    infocenter.list_nodes(
                        healthy_only=True,
                        tags=None,
                        limit=500,
                    )
                )
        except Exception:
            return []
        routes: List[InfoCenterServiceRoute] = []
        now = datetime.now(timezone.utc)
        lease_expire_at = now + timedelta(seconds=max(1.0, float(self.timeout_sec)))
        for node in nodes:
            if bool(getattr(node, "drain", False)):
                continue
            services = tuple(getattr(node, "services", ()) or ())
            for svc in services:
                if str(getattr(svc, "service_name", "") or "").strip() != self.service_name:
                    continue
                http_base_url = str(getattr(svc, "http_base_url", "") or "").strip()
                control_addr = str(getattr(node, "control_addr", "") or "").strip()
                if not http_base_url or not control_addr:
                    continue
                status = int(getattr(svc, "status", 0) or 0)
                if status not in {pb2.SERVICE_STATUS_RUNNING, pb2.SERVICE_STATUS_STARTING}:
                    continue
                worker_count = max(1, int(getattr(svc, "worker_count", 0) or 1))
                alive_workers = max(1, int(getattr(svc, "alive_workers", 0) or worker_count))
                in_flight = max(0, int(getattr(svc, "in_flight", 0) or 0))
                routes.append(
                    InfoCenterServiceRoute(
                        service_name=self.service_name,
                        service_id=str(getattr(svc, "service_id", "") or ""),
                        status=status,
                        node_instance_id=str(getattr(node, "node_instance_id", "") or getattr(node, "node_id", "") or ""),
                        node_id=str(getattr(node, "node_id", "") or ""),
                        control_addr=control_addr,
                        node_healthy=bool(getattr(node, "healthy", True)),
                        worker_count=worker_count,
                        alive_workers=alive_workers,
                        in_flight=in_flight,
                        lease_expire_at=lease_expire_at,
                        http_base_url=http_base_url,
                        reported_in_flight=in_flight,
                        received_count=0,
                        returned_count=0,
                        ema_child_invoke_ms=0.0,
                        ema_samples=0,
                        predicted_busy=float(in_flight) / float(alive_workers),
                        policy_id=str(getattr(svc, "policy_id", "") or get_default_policy_id_for_binding("service_internal")),
                    )
                )
        routes.sort(key=lambda route: _route_sort_key(route, strategy="predicted_busy"))
        return routes

    def _effective_worker_count(self) -> int:
        routes: List[object] = []
        if self.route == "discovery":
            with contextlib.suppress(Exception):
                routes = list(self._discoverable_routes(force_refresh=False))
        else:
            status = self._last_status if isinstance(self._last_status, dict) else None
            if not status:
                with contextlib.suppress(Exception):
                    fetched = self._transport_client.get_status(service_name=self.service_name)
                    if isinstance(fetched, dict):
                        self._last_status = fetched
                        status = fetched
            if isinstance(status, dict):
                routes = list(status.get("routes", []) or [])
        total_workers = sum(_service_route_worker_count(route) for route in routes)
        return max(1, total_workers)

    def _default_max_in_flight(self) -> int:
        return _scaled_default_max_in_flight(self._effective_worker_count())

    def _get_async_call_gate(self) -> asyncio.Semaphore:
        loop = asyncio.get_running_loop()
        capacity = max(1, int(self._default_max_in_flight()))
        if (
            self._async_call_gate is None
            or self._async_call_gate_loop is not loop
            or self._async_call_gate_capacity != capacity
        ):
            self._async_call_gate = asyncio.Semaphore(capacity)
            self._async_call_gate_loop = loop
            self._async_call_gate_capacity = capacity
        return self._async_call_gate

    def _get_async_call_executor(self) -> ThreadPoolExecutor:
        capacity = max(1, int(self._default_max_in_flight()))
        current_executor = getattr(self, "_async_call_executor", None)
        if (
            current_executor is None
            or getattr(self, "_async_call_executor_capacity", 0) != capacity
        ):
            if current_executor is not None:
                with contextlib.suppress(Exception):
                    current_executor.shutdown(wait=False, cancel_futures=True)
            self._async_call_executor = ThreadPoolExecutor(
                max_workers=capacity,
                thread_name_prefix="service-call",
            )
            self._async_call_executor_capacity = capacity
        return self._async_call_executor

    def call_balanced(
        self,
        method: str,
        payload: Dict[str, object],
        *,
        timeout_sec: float = 60.0,
        strategy: str = "predicted_busy",
        refresh_status: bool = True,
        max_attempts: int = 0,
        serialization_mode: str = "",
    ) -> Tuple[str, Dict[str, object]]:
        del refresh_status, max_attempts
        if self.route == "local":
            effective_serialization_mode = LOCAL_IPC_SERIALIZATION_MODE
        else:
            self._ensure_effective_policy_loaded(force_refresh=(self.route == "discovery"))
            effective_serialization_mode = resolve_effective_serialization_mode(
                request_mode=serialization_mode,
                context="gateway_public" if self.route == "gateway" else "service_call",
                frozen_mode=self.serialization_mode,
            )
        if self.route == "discovery":
            route_cache = self._route_cache
            strategy_name, _profile = resolve_service_strategy(strategy)
            tried: Set[str] = set()
            attempt_count = 0
            failed_count = 0
            last_failed_route_id = ""
            last_error: Optional[Exception] = None
            token = getattr(self._transport_client, "service_token", "")

            def _select_route():
                if route_cache is not None:
                    return route_cache.select_route(
                        self.service_name,
                        exclude_service_ids=tried,
                        strategy=strategy_name,
                    )
                candidates = [
                    item
                    for item in self._discoverable_routes(force_refresh=True)
                    if str(getattr(item, "service_id", "") or "") not in tried
                ]
                if not candidates:
                    raise RuntimeError(f"no available route for service_name={self.service_name!r}")
                return sorted(candidates, key=lambda item: _route_sort_key(item, strategy=strategy_name))[0]

            def _has_alternative() -> bool:
                if route_cache is not None:
                    with contextlib.suppress(Exception):
                        cached = [
                            item
                            for item in route_cache.get_routes(self.service_name)
                            if str(getattr(item, "service_id", "") or "") not in tried
                        ]
                        if cached:
                            return True
                    with contextlib.suppress(Exception):
                        return bool(
                            [
                                item
                                for item in route_cache.refresh(self.service_name, force=True)
                                if str(getattr(item, "service_id", "") or "") not in tried
                            ]
                        )
                    return True
                with contextlib.suppress(Exception):
                    return bool(
                        [
                            item
                            for item in self._discoverable_routes(force_refresh=True)
                            if str(getattr(item, "service_id", "") or "") not in tried
                        ]
                    )
                return True

            def _record_observation(*, selected_route_id: str = "") -> None:
                if route_cache is None:
                    return
                recorder = getattr(route_cache, "record_call_observation", None)
                if callable(recorder):
                    with contextlib.suppress(Exception):
                        recorder(
                            self.service_name,
                            route_attempt_count=attempt_count,
                            failed_route_count=failed_count,
                            last_failed_route_id=last_failed_route_id,
                            selected_route_id=selected_route_id,
                        )

            def _call_route(selected_route: object) -> Tuple[str, Dict[str, object]]:
                prepared_payload = (
                    self._prepare_discovery_route_payload(selected_route, payload)
                    if self._prepare_discovery_payload_enabled
                    else dict(payload or {})
                )
                route_call_kwargs = {
                    "method": method,
                    "payload": prepared_payload,
                    "timeout_sec": max(0.1, float(timeout_sec)),
                    "service_token": token,
                }
                if (
                    str(effective_serialization_mode or "").strip()
                    and effective_serialization_mode != "legacy_v1"
                ):
                    route_call_kwargs["serialization_mode"] = effective_serialization_mode
                if self.effective_policy is not None:
                    route_call_kwargs["effective_policy"] = self.effective_policy
                resp = self._client_mod._call_route_http(selected_route, **route_call_kwargs)
                attach_locator = getattr(self._transport_client, "_attach_controlplane_locator", None)
                if callable(attach_locator):
                    resp = attach_locator(resp, route=selected_route)
                return self._client_mod._node_instance_key_from_route(selected_route), resp

            while True:
                try:
                    route = _select_route()
                except Exception as select_exc:
                    _record_observation()
                    if last_error is not None:
                        raise RuntimeError(str(last_error)) from last_error
                    raise RuntimeError(str(select_exc)) from select_exc
                route_id = str(getattr(route, "service_id", "") or "")
                if route_id in tried:
                    if route_cache is not None:
                        with contextlib.suppress(Exception):
                            route_cache.release_route(route)
                    _record_observation()
                    if last_error is not None:
                        raise RuntimeError(str(last_error)) from last_error
                    raise RuntimeError(f"no untried route for service_name={self.service_name!r}")
                tried.add(route_id)
                attempt_count += 1
                try:
                    node_id, response = _call_route(route)
                    if route_cache is not None:
                        with contextlib.suppress(Exception):
                            route_cache.mark_success(route)
                    _record_observation(selected_route_id=route_id)
                    return node_id, response
                except self._client_mod.DiscoveryCallError as exc:
                    last_error = exc
                    failure_kind = classify_service_error(exc, route_failure=self._client_mod._is_route_failure(exc))
                    if not should_failover(failure_kind, has_alternative_candidate=_has_alternative()):
                        if route_cache is not None:
                            with contextlib.suppress(Exception):
                                route_cache.release_route(route)
                        _record_observation()
                        raise RuntimeError(str(exc)) from exc
                    failed_count += 1
                    last_failed_route_id = route_id
                    if route_cache is not None:
                        with contextlib.suppress(Exception):
                            route_cache.mark_failure(route, str(exc))
                        with contextlib.suppress(Exception):
                            route_cache.refresh(self.service_name, force=True)
                    continue
                except Exception as exc:
                    last_error = exc
                    if not should_failover(STAGING_FAILED, has_alternative_candidate=_has_alternative()):
                        if route_cache is not None:
                            with contextlib.suppress(Exception):
                                route_cache.release_route(route)
                        _record_observation()
                        raise RuntimeError(str(exc)) from exc
                    failed_count += 1
                    last_failed_route_id = route_id
                    if route_cache is not None:
                        with contextlib.suppress(Exception):
                            route_cache.mark_failure(route, str(exc))
                        with contextlib.suppress(Exception):
                            route_cache.refresh(self.service_name, force=True)
                    continue
        response = self._transport_client.call(
            service_name=self.service_name,
            method=method,
            payload=payload,
            timeout_sec=timeout_sec,
            serialization_mode=effective_serialization_mode,
            effective_policy=self.effective_policy,
        )
        return self.route, response

    async def acall_balanced(
        self,
        method: str,
        payload: Dict[str, object],
        *,
        timeout_sec: float = 60.0,
        strategy: str = "predicted_busy",
        refresh_status: bool = True,
        max_attempts: int = 0,
        serialization_mode: str = "",
    ) -> Tuple[str, Dict[str, object]]:
        loop = asyncio.get_running_loop()
        async with self._get_async_call_gate():
            return await loop.run_in_executor(
                self._get_async_call_executor(),
                lambda: self.call_balanced(
                    method,
                    payload,
                    timeout_sec=timeout_sec,
                    strategy=strategy,
                    refresh_status=refresh_status,
                    max_attempts=max_attempts,
                    serialization_mode=serialization_mode,
                ),
            )

    async def acall_all(
        self,
        method: str,
        payload: Dict[str, object],
        *,
        timeout_sec: float = 60.0,
        max_concurrency: int = 100,
    ):
        del max_concurrency
        if isinstance(payload, list):
            if len(payload) != 1:
                raise ValueError(f"{self.route} connected service broadcast accepts exactly one payload")
            payload = dict(payload[0] or {})
        node_id = "local" if self.route == "local" else self.service_name
        try:
            called_node_id, response = await self.acall_balanced(
                method,
                dict(payload or {}),
                timeout_sec=timeout_sec,
                refresh_status=False,
            )
            return [(called_node_id or node_id, response, None)]
        except Exception as exc:
            return [(node_id, None, exc)]

    async def call(self, method: str, **kwargs) -> Dict[str, object]:
        """Asynchronously call a service method.

        Use ``call_sync(...)`` for the synchronous variant.
        """
        node_id, resp = await self.acall_balanced(method, kwargs, timeout_sec=self.timeout_sec)
        return _resolve_high_level_service_data(self, node_id=node_id, response=resp)

    async def call_async(self, method: str, **kwargs) -> Dict[str, object]:
        """Explicit alias for ``call(...)``."""
        return await self.call(method, **kwargs)

    def call_sync(self, method: str, **kwargs) -> Dict[str, object]:
        """Synchronously call a service method."""
        node_id, resp = self.call_balanced(method, kwargs, timeout_sec=self.timeout_sec)
        return _resolve_high_level_service_data(self, node_id=node_id, response=resp)

    def stream_call(
        self,
        method: str,
        payload: Dict[str, object],
        *,
        timeout_sec: float = 60.0,
        strategy: str = "predicted_busy",
        refresh_status: bool = True,
        serialization_mode: str = "",
    ):
        del refresh_status
        if self.route == "local":
            effective_serialization_mode = LOCAL_IPC_SERIALIZATION_MODE
        else:
            self._ensure_effective_policy_loaded(force_refresh=True)
            effective_serialization_mode = resolve_effective_serialization_mode(
                request_mode=serialization_mode,
                context="gateway_public" if self.route == "gateway" else "service_call",
                frozen_mode=self.serialization_mode,
            )
        if self.route == "gateway":
            def _gateway_iter():
                for event in self._transport_client.stream_call(
                    service_name=self.service_name,
                    method=method,
                    payload=payload,
                    timeout_sec=max(0.1, float(timeout_sec)),
                    serialization_mode=effective_serialization_mode,
                    effective_policy=self.effective_policy,
                ):
                    event_name = str(event.get("event", "") or "")
                    if event_name == "item":
                        item_data = event.get("data")
                        if is_inline_transport_carrier(item_data):
                            item_data = decode_inline_transport_carrier(
                                item_data,
                                context="service_result",
                            )
                        yield _resolve_high_level_service_data(
                            self,
                            node_id="gateway",
                            response={"data": item_data},
                        )
                        continue
                    if event_name == "done":
                        if bool(event.get("ok", False)):
                            return
                        raise RuntimeError(str(event.get("error", "service stream failed")))

            return _gateway_iter()

        if self.route == "local":
            def _local_iter():
                for event in self._transport_client.stream_call(
                    service_name=self.service_name,
                    method=method,
                    payload=payload,
                    timeout_sec=max(0.1, float(timeout_sec)),
                    serialization_mode=effective_serialization_mode,
                ):
                    event_name = str(event.get("event", "") or "")
                    if event_name == "item":
                        item_data = event.get("data")
                        if is_inline_transport_carrier(item_data):
                            item_data = decode_inline_transport_carrier(
                                item_data,
                                context="service_result",
                            )
                        yield _resolve_high_level_service_data(
                            self,
                            node_id="local",
                            response={"data": item_data},
                        )
                        continue
                    if event_name == "done":
                        if bool(event.get("ok", False)):
                            return
                        raise RuntimeError(str(event.get("error", "service stream failed")))

            return _local_iter()

        if self.route != "discovery":
            raise NotImplementedError(f"stream_call does not support route={self.route!r}")
        route = (
            self._route_cache.select_route(self.service_name, strategy=resolve_service_strategy(strategy)[0])
            if self._route_cache is not None
            else sorted(
                self._discoverable_routes(force_refresh=True),
                key=lambda item: _route_sort_key(item, strategy=resolve_service_strategy(strategy)[0]),
            )[0]
        )
        prepared_payload = self._prepare_discovery_route_payload(route, payload)
        token = getattr(self._transport_client, "service_token", "")

        def _iter():
            route_cache = self._route_cache
            try:
                for event in self._client_mod._iter_route_http_stream(
                    route,
                    method=method,
                    payload=prepared_payload,
                    timeout_sec=max(0.1, float(timeout_sec)),
                    service_token=token,
                    serialization_mode=effective_serialization_mode,
                    effective_policy=self.effective_policy,
                ):
                    event_name = str(event.get("event", "") or "")
                    if event_name == "item":
                        item_data = event.get("data")
                        if is_inline_transport_carrier(item_data):
                            item_data = decode_inline_transport_carrier(
                                item_data,
                                context="service_result",
                            )
                        pseudo_response = {"data": item_data}
                        yield _resolve_high_level_service_data(
                            self,
                            node_id=self._client_mod._node_instance_key_from_route(route),
                            response=pseudo_response,
                        )
                        continue
                    if event_name == "done":
                        if bool(event.get("ok", False)):
                            if route_cache is not None:
                                with contextlib.suppress(Exception):
                                    route_cache.mark_success(route)
                            return
                        raise RuntimeError(str(event.get("error", "service stream failed")))
            except Exception:
                if route_cache is not None:
                    with contextlib.suppress(Exception):
                        route_cache.mark_failure(route, "service stream failed")
                raise

        return _iter()

    def map_calls(
        self,
        method: str,
        payloads: Sequence[Dict[str, object]],
        *,
        timeout_sec: float = 30.0,
        strategy: str = "predicted_busy",
        refresh_status: bool = False,
        max_in_flight: Optional[int] = None,
        progress: ProgressOption = False,
        progress_interval_sec: float = 2.0,
    ) -> List[Optional[object]]:
        return [
            item.result if item.ok else None
            for item in _service_collect_item_calls(
                self,
                method=method,
                payloads=payloads,
                timeout_sec=timeout_sec,
                strategy=strategy,
                refresh_status=refresh_status,
                max_in_flight=max_in_flight,
                progress=progress,
                progress_interval_sec=progress_interval_sec,
            )
        ]

    async def amap_calls(
        self,
        method: str,
        payloads: Sequence[Dict[str, object]],
        *,
        timeout_sec: float = 30.0,
        strategy: str = "predicted_busy",
        refresh_status: bool = False,
        max_in_flight: Optional[int] = None,
        progress: ProgressOption = False,
        progress_interval_sec: float = 2.0,
    ) -> List[Optional[object]]:
        items = await _service_acollect_item_calls(
            self,
            method=method,
            payloads=payloads,
            timeout_sec=timeout_sec,
            strategy=strategy,
            refresh_status=refresh_status,
            max_in_flight=max_in_flight,
            progress=progress,
            progress_interval_sec=progress_interval_sec,
        )
        return [item.result if item.ok else None for item in items]

    def unordered_calls(
        self,
        method: str,
        payloads: Sequence[Dict[str, object]],
        *,
        timeout_sec: float = 30.0,
        strategy: str = "predicted_busy",
        refresh_status: bool = False,
        max_in_flight: Optional[int] = None,
        return_items: bool = False,
        progress: ProgressOption = False,
        progress_interval_sec: float = 2.0,
    ) -> Iterator[Union[Tuple[int, Optional[object]], ExecutionItem]]:
        for item in _service_iter_item_calls(
            self,
            method=method,
            payloads=payloads,
            timeout_sec=timeout_sec,
            strategy=strategy,
            refresh_status=refresh_status,
            max_in_flight=max_in_flight,
            progress=progress,
            progress_interval_sec=progress_interval_sec,
        ):
            yield item if return_items else (item.index, item.result if item.ok else None)

    async def aunordered_calls(
        self,
        method: str,
        payloads: Sequence[Dict[str, object]],
        *,
        timeout_sec: float = 30.0,
        strategy: str = "predicted_busy",
        refresh_status: bool = False,
        max_in_flight: Optional[int] = None,
        return_items: bool = False,
        progress: ProgressOption = False,
        progress_interval_sec: float = 2.0,
    ) -> AsyncIterator[Union[Tuple[int, Optional[object]], ExecutionItem]]:
        async for item in _service_aiter_item_calls(
            self,
            method=method,
            payloads=payloads,
            timeout_sec=timeout_sec,
            strategy=strategy,
            refresh_status=refresh_status,
            max_in_flight=max_in_flight,
            progress=progress,
            progress_interval_sec=progress_interval_sec,
        ):
            yield item if return_items else (item.index, item.result if item.ok else None)

    def iter_item_calls(
        self,
        method: str,
        payloads: Sequence[Dict[str, object]],
        *,
        timeout_sec: float = 30.0,
        strategy: str = "predicted_busy",
        refresh_status: bool = False,
        max_in_flight: Optional[int] = None,
        progress: ProgressOption = False,
        progress_interval_sec: float = 2.0,
    ) -> Iterator[ExecutionItem]:
        return _service_iter_item_calls(
            self,
            method=method,
            payloads=payloads,
            timeout_sec=timeout_sec,
            strategy=strategy,
            refresh_status=refresh_status,
            max_in_flight=max_in_flight,
            progress=progress,
            progress_interval_sec=progress_interval_sec,
        )

    async def aiter_item_calls(
        self,
        method: str,
        payloads: Sequence[Dict[str, object]],
        *,
        timeout_sec: float = 30.0,
        strategy: str = "predicted_busy",
        refresh_status: bool = False,
        max_in_flight: Optional[int] = None,
        progress: ProgressOption = False,
        progress_interval_sec: float = 2.0,
    ) -> AsyncIterator[ExecutionItem]:
        async for item in _service_aiter_item_calls(
            self,
            method=method,
            payloads=payloads,
            timeout_sec=timeout_sec,
            strategy=strategy,
            refresh_status=refresh_status,
            max_in_flight=max_in_flight,
            progress=progress,
            progress_interval_sec=progress_interval_sec,
        ):
            yield item

    def collect_item_calls(
        self,
        method: str,
        payloads: Sequence[Dict[str, object]],
        *,
        timeout_sec: float = 30.0,
        strategy: str = "predicted_busy",
        refresh_status: bool = False,
        max_in_flight: Optional[int] = None,
        progress: ProgressOption = False,
        progress_interval_sec: float = 2.0,
    ) -> List[ExecutionItem]:
        return _service_collect_item_calls(
            self,
            method=method,
            payloads=payloads,
            timeout_sec=timeout_sec,
            strategy=strategy,
            refresh_status=refresh_status,
            max_in_flight=max_in_flight,
            progress=progress,
            progress_interval_sec=progress_interval_sec,
        )

    async def acollect_item_calls(
        self,
        method: str,
        payloads: Sequence[Dict[str, object]],
        *,
        timeout_sec: float = 30.0,
        strategy: str = "predicted_busy",
        refresh_status: bool = False,
        max_in_flight: Optional[int] = None,
        progress: ProgressOption = False,
        progress_interval_sec: float = 2.0,
    ) -> List[ExecutionItem]:
        return await _service_acollect_item_calls(
            self,
            method=method,
            payloads=payloads,
            timeout_sec=timeout_sec,
            strategy=strategy,
            refresh_status=refresh_status,
            max_in_flight=max_in_flight,
            progress=progress,
            progress_interval_sec=progress_interval_sec,
        )

    async def call_all(self, method: str, **kwargs) -> List[Tuple[Optional[str], Optional[object], Optional[Exception]]]:
        results = await self.acall_all(method, kwargs)
        return _resolve_high_level_service_results(self, results=results)

    def __getattr__(self, name: str):
        if name.startswith("_"):
            raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")
        if name in _CONNECTED_SERVICE_OWNER_ONLY_METHODS:
            raise AttributeError(
                f"'{type(self).__name__}' object has no attribute '{name}'; "
                f"{name} is only available on owner service handles"
            )
        if self._discovered_methods is None:
            self._ensure_methods_discovered()
        if self._discovered_methods is not None and name not in self._discovered_methods:
            raise AttributeError(
                f"'{type(self).__name__}' has no method '{name}'. Available methods: {self._discovered_methods}"
            )
        proxy_strategy = "predicted_busy"
        return _CallProxy(
            method=name,
            group=self,
            timeout_sec=self.timeout_sec,
            strategy=proxy_strategy,
            refresh_status=False,
        )

    def __repr__(self) -> str:
        methods = self.methods if self._discovered_methods is not None else ["<not discovered>"]
        effective_policy_text = ""
        if self.effective_policy is not None:
            effective_policy_text = (
                f" effective_policy={self.effective_policy.policy_id}@v{self.effective_policy.version}"
            )
        return (
            f"<ConnectedService "
            f"service={self.service_name!r} "
            f"route={self.route} "
            f"protocol={self.protocol} "
            f"serialization_mode={self.serialization_mode} "
            f"{effective_policy_text} "
            f"methods={methods[:3]}{'...' if len(methods) > 3 else ''}>"
        )


@dataclass
class Service(ServiceExecutionSession):
    """A deployed service session spread across multiple NodeControl nodes."""

    owner_client_id: str
    service_name: str
    sessions: Dict[str, ServiceSessionClient]
    nodes: Dict[str, InfoCenterNode]
    failed: bool = False
    failures: Dict[str, str] = field(default_factory=dict)
    globals_digests: Dict[str, str] = field(default_factory=dict)
    breaker_enabled: bool = True
    breaker_failure_threshold: int = 3
    breaker_cooldown_sec: float = 5.0
    breaker_max_cooldown_sec: float = 120.0
    _clients: Dict[str, Any] = field(default_factory=dict, repr=False)
    _session_cache_file: Optional[Path] = field(default=None, repr=False)
    _session_cache_lock: Optional[_ServiceSessionFileLock] = field(default=None, repr=False)
    _delete_session_cache_on_close: bool = field(default=False, repr=False)
    _artifact_code_version: str = field(default="", repr=False)
    _route_index: int = field(default=0, repr=False)
    _route_lock: threading.Lock = field(default_factory=threading.Lock, repr=False)
    _breaker_states: Dict[str, CandidateBreakerState] = field(default_factory=dict, repr=False)
    _discovered_methods: Optional[List[str]] = field(default=None, repr=False)
    _closed: bool = field(default=False, repr=False)
    _async_call_gate: Optional[asyncio.Semaphore] = field(default=None, repr=False)
    _async_call_gate_loop: Optional[asyncio.AbstractEventLoop] = field(default=None, repr=False)
    _async_call_gate_capacity: int = field(default=0, repr=False)
    _async_call_executor: Optional[ThreadPoolExecutor] = field(default=None, repr=False)
    _async_call_executor_capacity: int = field(default=0, repr=False)
    _compensation_spec: Optional[Dict[str, Any]] = field(default=None, repr=False)
    _compensation_lock: threading.Lock = field(default_factory=threading.Lock, repr=False)
    _last_compensation_attempt_at: float = field(default=0.0, repr=False)
    _last_managed_globals: Optional[Dict[str, object]] = field(default=None, repr=False)
    serialization_mode: str = ""
    policy_id: InitVar[str] = ""
    _policy_id: str = field(default="", repr=False)
    effective_policy: Optional[EffectivePolicy] = field(default=None, repr=False)

    def _replica_handles(self) -> Dict[str, ExecutionReplicaHandle]:
        return self.sessions

    def execution_identity(self) -> SessionIdentity:
        first = next(iter(self.sessions.values()))
        return first.identity()

    def execution_binding(self) -> SessionBinding:
        first = next(iter(self.sessions.values()))
        return first.binding()

    def execution_snapshot(self):
        return super().snapshot()

    def execution_status(self):
        return super().status()

    def route_summary(self) -> List[Dict[str, object]]:
        routes: List[Dict[str, object]] = []
        for node_key, session in sorted(self.sessions.items()):
            node = self.nodes.get(node_key)
            control_addr = ""
            node_id = ""
            if node is not None:
                control_addr = str(node.control_addr or "")
                node_id = str(node.node_id or "")
            elif node_key in self._clients:
                control_addr = str(self._clients[node_key].target or "")
            routes.append(
                {
                    "node_instance_id": str(node_key or ""),
                    "node_id": node_id,
                    "control_addr": control_addr,
                    "service_name": str(self.service_name or ""),
                    "service_id": str(session.service_id or ""),
                    "http_base_url": str(session.http_base_url or ""),
                }
            )
        return routes

    def routes(self) -> List[Dict[str, object]]:
        return self.route_summary()

    def _configure_dynamic_compensation(self, spec: Dict[str, Any]) -> None:
        desired = max(0, int(spec.get("node_count", 0) or 0))
        if desired <= 0:
            self._compensation_spec = None
            return
        self._compensation_spec = dict(spec)

    @staticmethod
    def _is_retryable_compensation_failure(message: str) -> bool:
        text = str(message or "").strip().lower()
        if not text:
            return False
        permanent_markers = (
            "modulenotfounderror",
            "importerror",
            "syntaxerror",
            "dependency install failed",
            "method `",
            "not exported",
            "runtime mismatch",
        )
        if any(marker in text for marker in permanent_markers):
            return False
        retryable_markers = (
            "heartbeat",
            "timeout",
            "timed out",
            "connection",
            "temporarily",
            "refused",
            "reset",
            "unreachable",
            "service is stopped",
            "service not found",
        )
        return any(marker in text for marker in retryable_markers)

    def _after_keepalive_tick(self) -> None:
        spec = self._compensation_spec
        if not spec:
            return
        now = time.monotonic()
        interval_sec = max(5.0, float(spec.get("check_interval_sec", 15.0) or 15.0))
        if now - float(self._last_compensation_attempt_at or 0.0) < interval_sec:
            return
        self._last_compensation_attempt_at = now
        self.try_compensate_replicas()

    def try_compensate_replicas(self) -> int:
        spec = self._compensation_spec
        if not spec or self._closed:
            return 0
        if not self._compensation_lock.acquire(blocking=False):
            return 0
        try:
            desired = max(0, int(spec.get("node_count", 0) or 0))
            active = {str(node_id) for node_id in self._active_replica_ids if str(node_id)}
            if desired <= 0 or len(active) >= desired:
                return 0
            failed = {str(node_id) for node_id in self.failures.keys() if str(node_id)}
            retryable_failed = {
                node_id
                for node_id, message in self.failures.items()
                if self._is_retryable_compensation_failure(str(message or ""))
            }
            failed_by_base_node: Dict[str, str] = {}
            for node_id in failed:
                node = self.nodes.get(node_id)
                base_node_id = str(getattr(node, "node_id", "") or "").strip()
                if base_node_id:
                    failed_by_base_node.setdefault(base_node_id, node_id)
            excluded = set(active)
            with _infocenter_client(spec["infocenter_target"], timeout_sec=float(spec.get("timeout_sec", 10.0) or 10.0)) as infocenter:
                discovered_nodes = list(
                    infocenter.list_nodes(
                        healthy_only=bool(spec.get("healthy_only", True)),
                        tags=list(spec.get("tags") or ()),
                        limit=max(desired, int(spec.get("node_limit", 100) or 100)),
                    )
                )
            requested_instance_ids = [
                str(item).strip() for item in list(spec.get("node_instance_ids") or ()) if str(item).strip()
            ]
            requested_node_ids = [str(item).strip() for item in list(spec.get("node_ids") or ()) if str(item).strip()]
            if requested_instance_ids:
                node_map = {_node_instance_key_from_node(node): node for node in discovered_nodes}
                candidate_nodes = [node_map[node_id] for node_id in requested_instance_ids if node_id in node_map]
            elif requested_node_ids:
                node_map = _build_unique_node_id_map(discovered_nodes, requested_ids=requested_node_ids)
                candidate_nodes = [node_map[node_id] for node_id in requested_node_ids if node_id in node_map]
            else:
                candidate_nodes = [
                    node
                    for node in discovered_nodes
                    if is_admitted_node(node, require_control_addr=True)
                ]
                runtime = normalize_python_runtime_spec(str(spec.get("runtime", "") or ""))
                if runtime:
                    candidate_nodes = _filter_nodes_by_runtime(candidate_nodes, runtime=runtime)
                candidate_nodes.sort(
                    key=lambda node: (
                        -int(getattr(node, "service_worker_available", 0) or 0),
                        -int(getattr(node, "capacity", 0) or 0),
                        int(getattr(node, "queued", 0) or 0),
                        _node_instance_key_from_node(node),
                    )
                )
            candidates = [
                node
                for node in candidate_nodes
                if _node_instance_key_from_node(node) not in excluded
                and not (
                    _node_instance_key_from_node(node) in failed
                    and _node_instance_key_from_node(node) not in retryable_failed
                )
                and not (
                    _node_instance_key_from_node(node) in retryable_failed
                    and str(getattr(node, "node_id", "") or "").strip()
                    and failed_by_base_node.get(str(getattr(node, "node_id", "") or "").strip())
                    not in {"", _node_instance_key_from_node(node)}
                )
                and is_admitted_node(node, require_control_addr=True)
            ]
            if not candidates:
                return 0
            missing = max(0, desired - len(active))

            def _create_service_on_node(node: InfoCenterNode) -> Tuple[str, InfoCenterNode, Optional[Any], Optional[ServiceSessionClient], str]:
                node_key = _node_instance_key_from_node(node)
                try:
                    target = _node_control_target_for_node(node)
                    client = _new_node_control_client(target, timeout_sec=float(spec.get("timeout_sec", 10.0) or 10.0))
                except Exception as exc:
                    return node_key, node, None, None, repr(exc)
                node_worker_count = max(1, int(spec.get("worker_count", 1) or 1))
                if int(getattr(node, "service_worker_available", 0) or 0) > 0:
                    node_worker_count = max(1, min(node_worker_count, int(getattr(node, "service_worker_available", 0) or 0)))
                try:
                    session = client.create_service_from_bytes(
                        owner_client_id=self.owner_client_id,
                        service_name=self.service_name,
                        blob=spec.get("blob") or b"",
                        runtime=str(spec.get("runtime", "py3") or "py3"),
                        entry_module=str(spec.get("entry_module", "") or ""),
                        entry_callable=str(spec.get("entry_callable", "run") or "run"),
                        package_format=str(spec.get("package_format", "") or ""),
                        export_mode=str(spec.get("export_mode", "decorator") or "decorator"),
                        export_methods=list(spec.get("export_methods") or ()),
                        deps=spec.get("deps"),
                        managed_global_names=list(spec.get("managed_global_names") or ()),
                        initial_globals=dict(spec.get("initial_globals") or {}),
                        policy_id=str(spec.get("policy_id", "") or ""),
                        worker_count=node_worker_count,
                        heartbeat_timeout_sec=max(5, int(spec.get("heartbeat_timeout_sec", 30) or 30)),
                        idle_ttl_sec=max(0, int(spec.get("idle_ttl_sec", 0) or 0)),
                        expose_http=bool(spec.get("expose_http", True)),
                        chunk_size=max(1, int(spec.get("chunk_size", OBJECT_CHUNK_SIZE_BYTES) or OBJECT_CHUNK_SIZE_BYTES)),
                        api_token=str(spec.get("api_token", "") or ""),
                    )
                except Exception as exc:
                    client.close()
                    return node_key, node, None, None, repr(exc)
                session.node_instance_id = node_key
                session.node_id = str(node.node_id or "")
                return node_key, node, client, session, ""

            create_results = [_create_service_on_node(node) for node in candidates[:missing]]
            added = 0
            with self._route_lock:
                for node_key, node, client, session, error_message in create_results:
                    if error_message:
                        self.failures[node_key] = error_message
                        continue
                    if client is None or session is None:
                        continue
                    old_client = self._clients.pop(node_key, None)
                    if old_client is not None and old_client is not client:
                        with contextlib.suppress(Exception):
                            old_client.close()
                    self.sessions[node_key] = session
                    self._clients[node_key] = client
                    self.nodes[node_key] = node
                    self.failures.pop(node_key, None)
                    self._active_replica_ids.add(node_key)
                    self._breaker_states.setdefault(node_key, CandidateBreakerState())
                    added += 1
            if added:
                self._persist_session_cache()
                if self._last_managed_globals is not None:
                    self.update_globals(dict(self._last_managed_globals))
                _emit_owner_notice(
                    f"dynamic compensation added={added} target_nodes={desired} "
                    f"service_name={self.service_name} routes={_format_route_summary(self.route_summary())}"
                )
            return added
        finally:
            self._compensation_lock.release()

    @classmethod
    def startup(
        cls,
        *,
        source: Any = None,
        deps: Optional[Any] = None,
        service_name: str = "",
        entry_module: str = "",
        entry_callable: str = "run",
        export_methods: Optional[Sequence[str]] = None,
        bind: Optional[str] = None,
        target: str = "",
        control_addr: str = "",
        node_id: str = "",
        service_http_base_url: str = "",
        service_id: str = "",
        worker_count: int = 1,
        policy_id: str = "",
        runtime: str = "py3",
        package_format: str = "module",
        managed_global_names: Optional[Sequence[str]] = None,
        initial_globals: Optional[Dict[str, object]] = None,
        tags: Optional[Sequence[str]] = None,
        metadata: Optional[Dict[str, str]] = None,
        queue_capacity: int = 0,
        version: str = "",
        heartbeat_sec: int = 10,
        rpc_timeout_sec: float = 5.0,
        start: bool = True,
        replace_existing: bool = False,
    ):
        """Product-facing startup-mounted service action.

        This path is module-first: prefer passing a live Python module object
        (or `package_format="module"`) so startup can mount it directly and
        keep local behavior close to the runtime service shape.

        Use `deploy()` when you want the upload/artifact path instead.
        """
        from pycloud_parallel.controlplane.startup_service_node import StartupServiceNode

        module_name = str(entry_module or "").strip()
        if source is not None:
            if isinstance(source, str):
                module_name = str(source or "").strip()
            else:
                module_name = str(getattr(source, "__name__", "") or "").strip()
        if not module_name:
            raise ValueError("Service.startup() requires entry_module=... or source=module")
        effective_service_name = str(service_name or module_name.rsplit(".", 1)[-1] or "startup-service").strip()
        effective_node_id = str(node_id or f"{effective_service_name}-startup").strip()
        initial_globals_values, effective_managed_global_names = normalize_initial_globals(initial_globals, managed_global_names)
        service_http_bind = "0.0.0.0:0" if bind is None else str(bind).strip()
        normalized_package_format = str(package_format or "").strip().lower()
        direct_module_mount = normalized_package_format == "module" or (
            normalized_package_format == "" and inspect.ismodule(source)
        )
        prepared_artifact = None
        if not direct_module_mount:
            artifact_source = source
            if artifact_source is None or isinstance(artifact_source, str):
                artifact_source = importlib.import_module(module_name)
            normalized_artifact = _normalize_artifact_input(
                consumer_kind="service",
                source=artifact_source,
                deps=deps,
                runtime=runtime,
                entry_module=entry_module,
                entry_callable=entry_callable,
                package_format=package_format,
                exports=ArtifactExports.explicit(export_methods) if export_methods else ArtifactExports.export_all(),
                managed_global_names=effective_managed_global_names,
            )
            prepared_artifact = _prepare_artifact(
                normalized_artifact,
                consumer_kind="service",
            )

        def _bind_startup_gateway(startup_node) -> None:
            deadline = time.monotonic() + _STARTUP_PREFLIGHT_RETRY_SEC
            expected_endpoint = _startup_expected_endpoint(
                service_http_base_url=str(service_http_base_url or "").strip(),
                service_http_bind=service_http_bind,
            )
            while True:
                try:
                    if direct_module_mount:
                        startup_node.start_mounted_service_gateway()
                    else:
                        startup_node.start_node_service_gateway()
                    return
                except OSError as exc:
                    if time.monotonic() >= deadline:
                        endpoint_text = (
                            f"{expected_endpoint[0]}:{expected_endpoint[1]}"
                            if expected_endpoint[1] > 0
                            else service_http_bind
                        )
                        raise RuntimeError(
                            "startup service endpoint is already in use after retry: "
                            f"service_name={effective_service_name!r} endpoint={endpoint_text}"
                        ) from exc
                    time.sleep(_STARTUP_PREFLIGHT_SLEEP_SEC)

        def _preflight_existing_startup_service() -> None:
            if not start or not effective_infocenter_target:
                return
            expected_endpoint = _startup_expected_endpoint(
                service_http_base_url=str(service_http_base_url or "").strip(),
                service_http_bind=service_http_bind,
            )
            with _infocenter_client(effective_infocenter_target, timeout_sec=rpc_timeout_sec) as infocenter:
                list_routes = getattr(infocenter, "list_service_routes_for_exclusive_check", None)
                if callable(list_routes):
                    raw_routes = list_routes(
                        service_name=effective_service_name,
                        limit=100,
                    )
                else:
                    raw_routes = infocenter.list_service_routes(
                        service_name=effective_service_name,
                        healthy_only=True,
                        limit=100,
                    )
                routes = _startup_active_routes(raw_routes)
            if not routes:
                return
            if expected_endpoint[1] <= 0:
                raise RuntimeError(
                    "startup service_name already exists and current startup has no fixed endpoint: "
                    f"service_name={effective_service_name!r}"
                )
            mismatched = [
                route
                for route in routes
                if not _startup_endpoint_matches(_route_endpoint(route), expected_endpoint)
            ]
            if mismatched:
                details = ", ".join(
                    f"{route.node_id or route.node_instance_id}@{route.http_base_url or route.control_addr or '-'}"
                    for route in mismatched
                )
                raise RuntimeError(
                    "startup service_name already exists on a different endpoint: "
                    f"service_name={effective_service_name!r} current={expected_endpoint[0]}:{expected_endpoint[1]} "
                    f"existing=[{details}]"
                )

        local_mode = str(target or "").strip().lower() == "local"
        effective_infocenter_target = "" if local_mode else str(target or "").strip()
        if local_mode:
            service_http_bind = ""
            if replace_existing:
                from pycloud_parallel.controlplane.local_ipc import stop_local_service

                with contextlib.suppress(FileNotFoundError):
                    stop_local_service(effective_service_name, timeout_sec=rpc_timeout_sec, force=True)
        effective_worker_count = max(1, int(worker_count or 1))
        node = StartupServiceNode(
            node_id=effective_node_id,
            worker_capacity=effective_worker_count,
            service_worker_capacity=effective_worker_count,
            task_pool_worker_capacity=1,
            executor_poll_interval_sec=(
                _LOCAL_SERVICE_EXECUTOR_POLL_INTERVAL_SEC
                if local_mode
                else 0.05
            ),
            service_http_bind="",
            service_http_base_url=str(service_http_base_url or "").strip(),
            enable_internal_executor=False,
            enable_service_session=True,
            service_default_worker_count=effective_worker_count,
        )
        node.close_on_registration_lost = True
        node.install_interrupt_shutdown_handlers()
        try:
            node.service_http_bind = service_http_bind
            _preflight_existing_startup_service()
            if start and not local_mode:
                _bind_startup_gateway(node)
            if direct_module_mount:
                node.mount_python_module_service(
                    service_name=effective_service_name,
                    entry_module=module_name,
                    export_methods=export_methods,
                    service_id=service_id,
                    worker_count=effective_worker_count,
                    policy_id=policy_id or (get_default_policy_id_for_binding("service_internal") if local_mode else ""),
                    managed_global_names=effective_managed_global_names,
                )
                if initial_globals_values:
                    node.update_globals(initial_globals_values, service_id=service_id, service_name=effective_service_name)
            else:
                node.mount_prepared_service(
                    owner_client_id=f"{effective_node_id}-owner",
                    service_name=effective_service_name,
                    sha256=prepared_artifact.content_sha256,
                    runtime=prepared_artifact.runtime,
                    entry_module=prepared_artifact.entry_module,
                    entry_callable=prepared_artifact.entry_callable,
                    package_format=prepared_artifact.package_format,
                    export_mode=prepared_artifact.export_mode,
                    export_methods=list(prepared_artifact.export_methods),
                    export_decorator=prepared_artifact.export_decorator,
                    dependency_policy_mode=prepared_artifact.dependency_policy_mode,
                    dependency_allowlist=list(prepared_artifact.dependency_allowlist),
                    managed_global_names=list(prepared_artifact.managed_global_names),
                    initial_globals=initial_globals_values,
                    policy_id=policy_id or (get_default_policy_id_for_binding("service_internal") if local_mode else ""),
                    worker_count=effective_worker_count,
                    heartbeat_timeout_sec=max(5, int(heartbeat_sec or 5) * 3),
                    idle_ttl_sec=0,
                    expose_http=bool(start and not local_mode),
                    chunks=[prepared_artifact.blob],
                    service_id=service_id,
                )
            if local_mode:
                node.start_local_ipc()
            if effective_infocenter_target:
                node.start_infocenter_registration(
                    infocenter_target=effective_infocenter_target,
                    control_addr=control_addr,
                    queue_capacity=queue_capacity,
                    tags=tags,
                    version=version,
                    metadata={
                        "service_name": effective_service_name,
                        "entry_module": module_name,
                        **dict(metadata or {}),
                    },
                    heartbeat_sec=heartbeat_sec,
                    rpc_timeout_sec=rpc_timeout_sec,
                )
        except Exception:
            node.close()
            raise
        return node

    @classmethod
    def _deploy_local(
        cls,
        *,
        source: Any = None,
        owner_client_id: Optional[str] = None,
        service_name: Optional[str] = None,
        artifact: Optional[Any] = None,
        deps: Optional[Any] = None,
        runtime: str = "py3",
        package_format: str = "",
        serialization_mode: str = "",
        resource_paths: Optional[Sequence[Any]] = None,
        export_methods: Optional[Sequence[str]] = None,
        managed_global_names: Optional[Sequence[str]] = None,
        initial_globals: Optional[Dict[str, object]] = None,
        worker_count: int = 10,
        heartbeat_timeout_sec: int = 30,
        idle_ttl_sec: int = 0,
        chunk_size: int = OBJECT_CHUNK_SIZE_BYTES,
        timeout_sec: float = 10.0,
    ):
        del serialization_mode, chunk_size, timeout_sec
        from pycloud_parallel.controlplane.startup_service_node import StartupServiceNode
        initial_globals_values, effective_managed_global_names = normalize_initial_globals(initial_globals, managed_global_names)

        module_source = source if inspect.ismodule(source) else None
        normalized_resource_paths = [item for item in list(resource_paths or ()) if str(item or "").strip()]
        if normalized_resource_paths and module_source is None:
            raise ValueError("resource_paths requires a module source")
        entry_module: Any = ""
        entry_callable: Any = "run"
        if normalized_resource_paths and module_source is not None:
            module_blob, module_filename = _prepare_code_blob(module=module_source, resource_paths=normalized_resource_paths)
            source = module_blob
            entry_module = _default_entry_module_for_module(module_source)
            package_format = _resolve_package_format(package_format, module_filename, default="py")

        normalized_artifact = _normalize_artifact_input(
            consumer_kind="service",
            source=source,
            artifact=artifact,
            deps=deps,
            runtime=runtime,
            entry_module=entry_module,
            entry_callable=entry_callable,
            package_format=package_format,
            exports=ArtifactExports.explicit(export_methods) if export_methods else None,
            managed_global_names=effective_managed_global_names,
        )
        prepared_artifact = _prepare_artifact(normalized_artifact, consumer_kind="service")
        effective_entry_module = prepared_artifact.entry_module
        if not effective_entry_module and prepared_artifact.filename.endswith(".py"):
            effective_entry_module = Path(prepared_artifact.filename).stem
        local_ip = _get_local_ip()
        effective_owner = str(owner_client_id or f"local-client-{local_ip}").strip()
        effective_service_name = str(service_name or effective_entry_module or f"local-service-{uuid.uuid4().hex[:10]}").strip()
        if not effective_service_name:
            raise ValueError("service_name is required")
        effective_worker_count = max(1, int(worker_count or 1))
        node = StartupServiceNode(
            node_id=f"{effective_service_name}-local",
            worker_capacity=effective_worker_count,
            service_worker_capacity=effective_worker_count,
            task_pool_worker_capacity=1,
            executor_poll_interval_sec=_LOCAL_SERVICE_EXECUTOR_POLL_INTERVAL_SEC,
            service_http_bind="",
            enable_internal_executor=False,
            enable_service_session=True,
            service_default_worker_count=effective_worker_count,
        )
        try:
            node.mount_prepared_service(
                owner_client_id=effective_owner,
                service_name=effective_service_name,
                sha256=prepared_artifact.content_sha256,
                runtime=prepared_artifact.runtime,
                entry_module=prepared_artifact.entry_module,
                entry_callable=prepared_artifact.entry_callable,
                package_format=prepared_artifact.package_format,
                export_mode=prepared_artifact.export_mode,
                export_methods=list(prepared_artifact.export_methods),
                export_decorator=prepared_artifact.export_decorator,
                dependency_policy_mode=prepared_artifact.dependency_policy_mode,
                dependency_allowlist=list(prepared_artifact.dependency_allowlist),
                managed_global_names=list(prepared_artifact.managed_global_names),
                initial_globals=initial_globals_values,
                policy_id=get_default_policy_id_for_binding("service_internal"),
                worker_count=effective_worker_count,
                heartbeat_timeout_sec=max(5, int(heartbeat_timeout_sec or 30)),
                idle_ttl_sec=max(0, int(idle_ttl_sec or 0)),
                expose_http=False,
                chunks=[prepared_artifact.blob],
            )
            node.start_local_ipc()
        except Exception:
            node.close()
            raise
        _emit_owner_notice(f"local deploy success service_name={effective_service_name} methods={node.methods}")
        return node

    @classmethod
    def deploy(
        cls,
        *,
        target: str,
        source: Any = None,
        owner_client_id: Optional[str] = None,
        service_name: Optional[str] = None,
        artifact: Optional[Any] = None,
        deps: Optional[Any] = None,
        runtime: str = "py3",
        package_format: str = "",
        serialization_mode: str = "",
        resource_paths: Optional[Sequence[Any]] = None,
        export_methods: Optional[Sequence[str]] = None,
        managed_global_names: Optional[Sequence[str]] = None,
        initial_globals: Optional[Dict[str, object]] = None,
        worker_count: int = 10,
        heartbeat_timeout_sec: int = 30,
        idle_ttl_sec: int = 0,
        expose_http: bool = True,
        chunk_size: int = OBJECT_CHUNK_SIZE_BYTES,
        healthy_only: bool = True,
        tags: Optional[Sequence[str]] = None,
        node_ids: Optional[Sequence[str]] = None,
        node_instance_ids: Optional[Sequence[str]] = None,
        node_count: int = 0,
        node_limit: int = 100,
        allow_partial: bool = True,
        min_success_nodes: int = 1,
        timeout_sec: float = 10.0,
        ensure_unique_service_name: bool = True,
        reuse_existing_same_code: bool = True,
        replace_existing_if_code_changed: bool = True,
        session_cache_dir: str = "",
        breaker_enabled: bool = True,
        breaker_failure_threshold: int = 3,
        breaker_cooldown_sec: float = 5.0,
        breaker_max_cooldown_sec: float = 120.0,
        api_token: str = "",
    ) -> "Service":
        """Product-facing deploy action for V1 service sessions.

        Default path: ``Service.deploy(target=\"127.0.0.1:50051\", source=my_module, ...)``.
        Advanced path: ``Service.deploy(artifact=Artifact(...), ...)``.
        """
        if str(target or "").strip().lower() == "local":
            return cls._deploy_local(
                source=source,
                owner_client_id=owner_client_id,
                service_name=service_name,
                artifact=artifact,
                deps=deps,
                runtime=runtime,
                package_format=package_format,
                serialization_mode=serialization_mode,
                resource_paths=resource_paths,
                export_methods=export_methods,
                managed_global_names=managed_global_names,
                initial_globals=initial_globals,
                worker_count=worker_count,
                heartbeat_timeout_sec=heartbeat_timeout_sec,
                idle_ttl_sec=idle_ttl_sec,
                chunk_size=chunk_size,
                timeout_sec=timeout_sec,
            )
        effective_target = _resolve_public_target_arg(
            target=target,
            action_name="Service.deploy()",
        )
        return cls._deploy_from_infocenter(
            infocenter_target=effective_target,
            source=source,
            owner_client_id=owner_client_id,
            service_name=service_name,
            artifact=artifact,
            deps=deps,
            runtime=runtime,
            package_format=package_format,
            serialization_mode=serialization_mode,
            resource_paths=resource_paths,
            export_methods=export_methods,
            managed_global_names=managed_global_names,
            initial_globals=initial_globals,
            worker_count=worker_count,
            heartbeat_timeout_sec=heartbeat_timeout_sec,
            idle_ttl_sec=idle_ttl_sec,
            expose_http=expose_http,
            chunk_size=chunk_size,
            healthy_only=healthy_only,
            tags=tags,
            node_ids=node_ids,
            node_instance_ids=node_instance_ids,
            node_count=node_count,
            node_limit=node_limit,
            allow_partial=allow_partial,
            min_success_nodes=min_success_nodes,
            timeout_sec=timeout_sec,
            ensure_unique_service_name=ensure_unique_service_name,
            reuse_existing_same_code=reuse_existing_same_code,
            replace_existing_if_code_changed=replace_existing_if_code_changed,
            session_cache_dir=session_cache_dir,
            breaker_enabled=breaker_enabled,
            breaker_failure_threshold=breaker_failure_threshold,
            breaker_cooldown_sec=breaker_cooldown_sec,
            breaker_max_cooldown_sec=breaker_max_cooldown_sec,
            api_token=api_token,
        )

    @classmethod
    def connect(
        cls,
        *,
        target: str,
        service_name: str,
        timeout_sec: float = 10.0,
        service_token: str = "",
        route: str = "discovery",
        protocol: str = "http",
        serialization_mode: str = "",
        validate_on_init: bool = False,
    ):
        """Product-facing connect action for an already deployed service."""
        return cls._connect_route(
            target=target,
            service_name=service_name,
            timeout_sec=timeout_sec,
            service_token=service_token,
            route=route,
            protocol=protocol,
            serialization_mode=serialization_mode,
            validate_on_init=validate_on_init,
        )

    @classmethod
    def _connect_route(
        cls,
        *,
        target: str,
        service_name: str,
        timeout_sec: float = 10.0,
        service_token: str = "",
        route: str = "discovery",
        protocol: str = "http",
        serialization_mode: str = "",
        validate_on_init: bool = False,
        effective_policy_override: Optional[EffectivePolicy] = None,
        prepare_discovery_payload: bool = True,
    ):
        normalized_protocol = str(protocol or "http").strip().lower() or "http"
        if normalized_protocol != "http":
            raise ValueError("Service.connect() protocol must be 'http'")
        normalized_route = str(route or "discovery").strip().lower() or "discovery"
        if str(target or "").strip().lower() == "local":
            from pycloud_parallel.controlplane.local_ipc import LocalServiceClient

            return _ConnectedService(
                transport_client=LocalServiceClient(
                    service_name=service_name,
                    timeout_sec=timeout_sec,
                ),
                service_name=service_name,
                route="local",
                protocol=normalized_protocol,
                timeout_sec=timeout_sec,
                serialization_mode=LOCAL_IPC_SERIALIZATION_MODE,
                validate_on_init=validate_on_init,
                effective_policy_override=effective_policy_override,
                prepare_discovery_payload=prepare_discovery_payload,
            )
        if normalized_route == "gateway":
            from pycloud_parallel.controlplane.gateway_client import GatewayServiceClient

            return _ConnectedService(
                transport_client=GatewayServiceClient(
                    target,
                    timeout_sec=timeout_sec,
                    service_token=service_token,
                ),
                service_name=service_name,
                route=normalized_route,
                protocol=normalized_protocol,
                timeout_sec=timeout_sec,
                serialization_mode=serialization_mode,
                validate_on_init=validate_on_init,
                effective_policy_override=effective_policy_override,
                prepare_discovery_payload=prepare_discovery_payload,
            )
        if normalized_route != "discovery":
            raise ValueError(
                "Service.connect() route must be one of: discovery, gateway"
            )
        from pycloud_parallel.controlplane.discovery_client import DiscoveryServiceClient

        return _ConnectedService(
            transport_client=DiscoveryServiceClient(
                target,
                timeout_sec=timeout_sec,
                service_token=service_token,
                shared_route_cache=True,
            ),
            service_name=service_name,
            route=normalized_route,
            protocol=normalized_protocol,
            timeout_sec=timeout_sec,
            serialization_mode=serialization_mode,
            validate_on_init=validate_on_init,
            effective_policy_override=effective_policy_override,
            prepare_discovery_payload=prepare_discovery_payload,
        )

    @classmethod
    def _deploy_from_infocenter(
        cls,
        *,
        infocenter_target: str,
        source: Any = None,
        owner_client_id: Optional[str] = None,
        service_name: Optional[str] = None,
        artifact: Optional[Any] = None,
        deps: Optional[Any] = None,
        runtime: str = "py3",
        entry_module: Any = "",
        entry_callable: Any = "run",
        package_format: str = "",
        serialization_mode: str = "",
        resource_paths: Optional[Sequence[Any]] = None,
        export_methods: Optional[Sequence[str]] = None,
        managed_global_names: Optional[Sequence[str]] = None,
        initial_globals: Optional[Dict[str, object]] = None,
        worker_count: int = 10,
        heartbeat_timeout_sec: int = 30,
        idle_ttl_sec: int = 0,
        expose_http: bool = True,
        chunk_size: int = OBJECT_CHUNK_SIZE_BYTES,
        healthy_only: bool = True,
        tags: Optional[Sequence[str]] = None,
        node_ids: Optional[Sequence[str]] = None,
        node_instance_ids: Optional[Sequence[str]] = None,
        node_count: int = 0,
        node_limit: int = 100,
        allow_partial: bool = True,
        min_success_nodes: int = 1,
        timeout_sec: float = 10.0,
        ensure_unique_service_name: bool = True,
        reuse_existing_same_code: bool = True,
        replace_existing_if_code_changed: bool = True,
        session_cache_dir: str = "",
        breaker_enabled: bool = True,
        breaker_failure_threshold: int = 3,
        breaker_cooldown_sec: float = 5.0,
        breaker_max_cooldown_sec: float = 120.0,
        policy_id: str = "",
        api_token: str = "",
    ) -> "Service":
        """Internal low-level deploy implementation behind ``Service.deploy(...)``.

        `policy_id` here is a deployment/control-plane input. Product callers
        should normally express only `serialization_mode`; the session will
        expose the frozen `effective_policy` that actually took effect.

        Args:
            infocenter_target: InfoCenter 地址
            source: 默认产品化代码输入；可传 callable / module / path / bytes
            owner_client_id: 所有者 ID
            service_name: 服务名称
            artifact: 高级 Artifact 声明对象
            runtime: 运行时版本
            entry_module: 入口模块名，或可导入的真实模块对象
            entry_callable: 入口函数名，或真实函数对象
            package_format: 包格式 ("py", "zip", "tar.gz")
            worker_count: 工作进程数
            heartbeat_timeout_sec: 心跳超时
            idle_ttl_sec: 空闲 TTL
            expose_http: 是否暴露 HTTP
            chunk_size: 上传分片大小
            healthy_only: 是否只使用健康节点
            tags: 节点标签过滤
            node_ids: 显式指定要部署到哪些节点
            node_count: 需要挑选的节点数量；未指定时默认使用 min_success_nodes
            node_limit: 节点数量限制
            allow_partial: 是否允许部分失败
            min_success_nodes: 最小成功节点数
            timeout_sec: 超时时间
            ensure_unique_service_name: 是否确保服务名唯一
            reuse_existing_same_code: 同 owner + 同代码时是否直接复用已存在服务
            replace_existing_if_code_changed: 同 owner + 同服务名但代码变化时是否替换（默认自动替换）
            session_cache_dir: 本地 service session token 缓存目录
            breaker_enabled: 是否启用熔断器
            breaker_failure_threshold: 熔断失败阈值
            breaker_cooldown_sec: 熔断冷却时间
            breaker_max_cooldown_sec: 熔断最大冷却时间

        Returns:
            Service: 部署的服务组
        """
        effective_api_token = _resolve_owner_api_token(api_token)
        initial_globals_values, effective_managed_global_names = normalize_initial_globals(initial_globals, managed_global_names)
        prepared_artifact = prepare_deployment_artifact(
            consumer_kind="service",
            source=source,
            artifact=artifact,
            deps=deps,
            runtime=runtime,
            entry_module=entry_module,
            entry_callable=entry_callable,
            package_format=package_format,
            managed_global_names=effective_managed_global_names,
            export_methods=export_methods,
            resource_paths=resource_paths,
        )
        effective_blob = prepared_artifact.blob
        effective_filename = prepared_artifact.filename
        runtime = prepared_artifact.runtime
        effective_entry_module = prepared_artifact.entry_module
        entry_callable = prepared_artifact.entry_callable
        effective_package_format = prepared_artifact.package_format
        export_mode = prepared_artifact.export_mode
        export_methods = list(prepared_artifact.export_methods)
        dependency_allowlist = list(prepared_artifact.dependency_allowlist)
        managed_global_names = list(prepared_artifact.managed_global_names)
        requested_policy_id = str(policy_id or "").strip().lower()
        normalized_policy_id = requested_policy_id or get_default_policy_id_for_binding("service_internal")

        # 生成默认的 owner_client_id 和 service_name
        local_ip = _get_local_ip()

        # 如果 owner_client_id 为空，使用本机 IP
        effective_owner_client_id = owner_client_id
        if not effective_owner_client_id:
            effective_owner_client_id = f"client-{local_ip}"

        # 先确定 entry_module（用于生成 service_name）
        if not effective_entry_module:
            if effective_filename:
                # 优先使用推导出的 artifact 文件名
                if effective_filename.endswith(".py"):
                    effective_entry_module = Path(effective_filename).stem

        # 如果 service_name 为空，使用 entry_module + 本机 IP + 时间戳（精确到秒）
        # 添加时间戳确保唯一性，避免服务名冲突
        effective_service_name = service_name
        if not effective_service_name:
            # 生成时间戳（精确到秒）
            timestamp = time.strftime("%Y%m%d%H%M%S")  # 格式: 20250330120000

            if effective_entry_module:
                effective_service_name = f"{effective_entry_module}-{local_ip}-{timestamp}"
            else:
                effective_service_name = f"service-{local_ip}-{timestamp}"

        # 现在才进行校验
        if not effective_owner_client_id:
            raise ValueError("owner_client_id is required")
        if not effective_service_name:
            raise ValueError("service_name is required")

        effective_code_version = prepared_artifact.code_version
        session_cache_file = _service_session_cache_file(
            owner_client_id=effective_owner_client_id,
            service_name=effective_service_name,
            cache_dir=session_cache_dir,
        )
        session_cache_lock: Optional[_ServiceSessionFileLock] = None

        requested_node_ids = [str(node_id).strip() for node_id in (node_ids or []) if str(node_id).strip()]
        requested_node_instance_ids = [str(node_id).strip() for node_id in (node_instance_ids or []) if str(node_id).strip()]
        desired_node_count = max(0, int(node_count or 0))
        compensation_target_count = desired_node_count or len(requested_node_instance_ids) or len(requested_node_ids)
        required_success_nodes = max(1, int(min_success_nodes))
        discovery_limit = max(
            1,
            int(node_limit),
            len(requested_node_ids),
            len(requested_node_instance_ids),
            desired_node_count or required_success_nodes,
        )

        _emit_owner_notice(
            "deploy start "
            f"service_name={effective_service_name} owner={effective_owner_client_id} "
            f"target={infocenter_target} runtime={runtime} "
            f"requested_node_ids={requested_node_ids or 'auto'} "
            f"requested_node_instance_ids={requested_node_instance_ids or 'auto'} "
            f"min_success_nodes={required_success_nodes}"
        )

        def _discover_from_infocenter() -> Tuple[Sequence[InfoCenterServiceRoute], Sequence[InfoCenterNode]]:
            with _infocenter_client(infocenter_target, timeout_sec=timeout_sec) as infocenter:
                existing_routes: Sequence[InfoCenterServiceRoute] = ()
                if ensure_unique_service_name:
                    list_routes = getattr(infocenter, "list_service_routes_for_exclusive_check", None)
                    if callable(list_routes):
                        existing_routes = list_routes(
                            service_name=effective_service_name,
                            limit=max(100, discovery_limit * 10),
                        )
                    else:
                        existing_routes = infocenter.list_service_routes(
                            service_name=effective_service_name,
                            healthy_only=True,
                            limit=max(100, discovery_limit * 10),
                        )
                discovered_nodes = infocenter.list_nodes(
                    healthy_only=healthy_only,
                    tags=tags,
                    limit=discovery_limit,
                )
                return existing_routes, discovered_nodes

        normalized_runtime = normalize_python_runtime_spec(runtime)

        def _select_nodes_from_discovery(
            existing_routes: Sequence[InfoCenterServiceRoute],
            discovered_nodes: Sequence[InfoCenterNode],
        ) -> Sequence[InfoCenterNode]:
            if not discovered_nodes:
                raise _RetryableReadyError(
                    f"no available nodes from InfoCenter: target={infocenter_target} "
                    f"healthy_only={healthy_only} tags={list(tags or ())}"
                )

            discovered_instance_map = {_node_instance_key_from_node(node): node for node in discovered_nodes}

            def _reject_non_deploy_nodes(nodes: Sequence[InfoCenterNode], *, scope: str) -> None:
                rejected = [(node, node_admission_block_reason(node, require_control_addr=True)) for node in nodes]
                rejected = [(node, reason) for node, reason in rejected if reason]
                if rejected:
                    details = ", ".join(
                        f"{node.node_id}/{_node_instance_key_from_node(node)}"
                        f"(accept_deploy={'yes' if getattr(node, 'accept_service_deploy', True) else 'no'},"
                        f" control_addr={str(getattr(node, 'control_addr', '') or '') or '-'},"
                        f" reason={reason})"
                        for node, reason in rejected
                    )
                    raise RuntimeError(f"{scope} contains nodes that do not accept service deployment: {details}")

            if requested_node_instance_ids:
                missing_node_instance_ids = [
                    node_id for node_id in requested_node_instance_ids if node_id not in discovered_instance_map
                ]
                if missing_node_instance_ids:
                    raise _RetryableReadyError(
                        f"requested node_instance_ids not found in current discovery scope: {missing_node_instance_ids}"
                    )
                selected_nodes = [discovered_instance_map[node_id] for node_id in requested_node_instance_ids]
                _reject_non_deploy_nodes(selected_nodes, scope="requested_node_instance_ids")
                if normalized_runtime:
                    incompatible = [
                        node
                        for node in selected_nodes
                        if str(node.python_version or "").strip()
                        and not matches_python_runtime(node.python_version, normalized_runtime)
                    ]
                    if incompatible:
                        raise RuntimeError(
                            runtime_mismatch_message_for_nodes(
                                requested_runtime=normalized_runtime,
                                nodes=incompatible,
                                scope="requested_node_instance_ids",
                            )
                        )
                return selected_nodes

            if requested_node_ids:
                discovered_node_map = _build_unique_node_id_map(discovered_nodes, requested_ids=requested_node_ids)
                missing_node_ids = [node_id for node_id in requested_node_ids if node_id not in discovered_node_map]
                if missing_node_ids:
                    raise _RetryableReadyError(
                        f"requested node_ids not found in current discovery scope: {missing_node_ids}"
                    )
                selected_nodes = [discovered_node_map[node_id] for node_id in requested_node_ids]
                _reject_non_deploy_nodes(selected_nodes, scope="requested_node_ids")
                if normalized_runtime:
                    incompatible = [
                        node
                        for node in selected_nodes
                        if str(node.python_version or "").strip()
                        and not matches_python_runtime(node.python_version, normalized_runtime)
                    ]
                    if incompatible:
                        raise RuntimeError(
                            runtime_mismatch_message_for_nodes(
                                requested_runtime=normalized_runtime,
                                nodes=incompatible,
                                scope="requested_node_ids",
                            )
                        )
                return selected_nodes

            candidate_nodes = [
                node
                for node in discovered_nodes
                if is_admitted_node(node, require_control_addr=True)
            ]
            if normalized_runtime:
                candidate_nodes = _filter_nodes_by_runtime(candidate_nodes, runtime=normalized_runtime)
            if not candidate_nodes:
                if normalized_runtime:
                    raise RuntimeError(
                        runtime_mismatch_message_for_nodes(
                            requested_runtime=normalized_runtime,
                            nodes=discovered_nodes,
                            scope="nodes",
                        )
                    )
                raise _RetryableReadyError(
                    f"no schedulable nodes from InfoCenter; target={infocenter_target}; "
                    f"candidates={_summarize_discovered_nodes(discovered_nodes)}"
                )
            candidate_nodes.sort(
                key=lambda node: (
                    -int(node.service_worker_available),
                    -int(node.capacity),
                    int(node.queued),
                    _node_instance_key_from_node(node),
                )
            )
            effective_node_count = max(1, desired_node_count or required_success_nodes)
            selected_nodes = candidate_nodes[:effective_node_count]
            if len(selected_nodes) < required_success_nodes:
                raise _RetryableReadyError(
                    "not enough schedulable nodes from InfoCenter: "
                    f"selected={len(selected_nodes)} required={required_success_nodes}"
                )
            return selected_nodes

        def _discover_and_select_nodes() -> Tuple[Sequence[InfoCenterServiceRoute], Sequence[InfoCenterNode], Sequence[InfoCenterNode]]:
            existing_routes, discovered_nodes = _discover_from_infocenter()
            selected_nodes = _select_nodes_from_discovery(existing_routes, discovered_nodes)
            return existing_routes, discovered_nodes, selected_nodes

        discovery_wait_timeout = _ready_retry_timeout(timeout_sec, grace_sec=_SERVICE_READY_GRACE_SEC)
        try:
            if discovery_wait_timeout > 0.0:
                selection_result = _retry_infocenter_request(
                    _discover_and_select_nodes,
                    timeout_sec=discovery_wait_timeout,
                    target=infocenter_target,
                    action="service deployment discovery",
                    retry_interval_sec=_SERVICE_READY_RETRY_INTERVAL_SEC,
                )
                if isinstance(selection_result, tuple) and len(selection_result) == 3:
                    existing_routes, discovered_nodes, selected_nodes = selection_result
                elif isinstance(selection_result, tuple) and len(selection_result) == 2:
                    existing_routes, discovered_nodes = selection_result
                    selected_nodes = _select_nodes_from_discovery(existing_routes, discovered_nodes)
                else:
                    raise RuntimeError(f"unexpected service discovery result: {selection_result!r}")
            else:
                existing_routes, discovered_nodes, selected_nodes = _discover_and_select_nodes()
        except RuntimeError as exc:
            message = str(exc)
            if "no available nodes" in message:
                _emit_owner_notice(
                    "deploy failed: no available nodes "
                    f"target={infocenter_target} healthy_only={healthy_only} tags={list(tags or ())}"
                )
            elif "no schedulable nodes" in message or "not enough schedulable nodes" in message:
                _emit_owner_notice(
                    "deploy failed: no schedulable nodes "
                    f"target={infocenter_target} candidates=retry_exhausted"
                )
            raise

        discovered_instance_map = {_node_instance_key_from_node(node): node for node in discovered_nodes}
        effective_policy = _service_effective_policy_for_nodes(
            selected_nodes,
            policy_id=normalized_policy_id,
            requested_mode=serialization_mode,
            context="service_owner",
        )

        if ensure_unique_service_name:
            active_routes = cls._select_active_routes(existing_routes)
            if active_routes:
                existing_infos = cls._inspect_existing_routes(active_routes=active_routes, timeout_sec=timeout_sec)
                existing_infos = [
                    (route, info)
                    for route, info in existing_infos
                    if cls._is_active_service_status(getattr(info, "status", route.status))
                ]
                if not existing_infos:
                    _emit_owner_notice(
                        f"ignore stale existing routes service_name={effective_service_name}; redeploying fresh replicas"
                    )
                else:
                    existing_owners = {info.owner_client_id for _, info in existing_infos}
                    existing_versions = {info.code_version for _, info in existing_infos}
                    if len(existing_owners) != 1 or len(existing_versions) != 1:
                        raise RuntimeError(
                            f"service_name already exists but active routes are inconsistent: {effective_service_name}"
                        )
                    existing_bound_policy_id = _resolve_bound_service_policy_id(
                        [route for route, _ in existing_infos],
                        default_policy_id=normalized_policy_id,
                        context=f"service_name={effective_service_name!r}",
                    )
                    if requested_policy_id and requested_policy_id != existing_bound_policy_id:
                        raise RuntimeError(
                            f"service_name already exists with deploy-bound policy_id={existing_bound_policy_id!r}; "
                            f"requested policy_id={requested_policy_id!r} does not match"
                        )

                    existing_owner = next(iter(existing_owners))
                    existing_code_version = next(iter(existing_versions))
                    if existing_owner != effective_owner_client_id:
                        raise RuntimeError(
                            f"service_name already exists and belongs to another owner: "
                            f"service_name={effective_service_name}; owner={existing_owner}"
                        )

                    cached_session = _load_service_session_cache(
                        owner_client_id=effective_owner_client_id,
                        service_name=effective_service_name,
                        cache_dir=session_cache_dir,
                    )

                    if existing_code_version == effective_code_version:
                        if not reuse_existing_same_code:
                            raise RuntimeError(
                                f"service_name already exists with same code_version: {effective_service_name}; "
                                "set reuse_existing_same_code=True to reuse"
                            )
                        if cached_session is None or cached_session.get("artifact_code_version") != effective_code_version:
                            raise RuntimeError(
                                f"service_name already exists with same code_version but no reusable local token cache was found: "
                                f"{effective_service_name}"
                            )
                        try:
                            session_cache_lock = _acquire_service_session_lock_with_retry(
                                session_cache_file,
                                timeout_sec=timeout_sec,
                                action=(
                                    "another local deploy process is already active for "
                                    f"owner_client_id={effective_owner_client_id!r} service_name={effective_service_name!r}"
                                ),
                            )
                        except RuntimeError as exc:
                            raise RuntimeError(str(exc)) from exc
                        try:
                            group = cls._reuse_existing_group(
                                owner_client_id=effective_owner_client_id,
                                service_name=effective_service_name,
                                artifact_code_version=effective_code_version,
                                cache_payload=cached_session,
                                active_routes=existing_infos,
                                discovered_node_map=discovered_instance_map,
                                timeout_sec=timeout_sec,
                                breaker_enabled=breaker_enabled,
                                breaker_failure_threshold=breaker_failure_threshold,
                                breaker_cooldown_sec=breaker_cooldown_sec,
                                breaker_max_cooldown_sec=breaker_max_cooldown_sec,
                                session_cache_file=session_cache_file,
                                session_cache_lock=session_cache_lock,
                                policy_id=existing_bound_policy_id,
                            )
                        except RuntimeError as exc:
                            if "service is stopped" not in str(exc):
                                raise
                            session_cache_lock = None
                            with contextlib.suppress(Exception):
                                session_cache_file.unlink()
                            _emit_owner_notice(
                                f"reuse existing service skipped because cached route stopped: {effective_service_name}; redeploying"
                            )
                        else:
                            _emit_owner_notice(
                                f"reuse existing service service_name={effective_service_name} "
                                f"routes={_format_route_summary(group.route_summary())}"
                            )
                            return group

                    else:
                        if not replace_existing_if_code_changed:
                            raise RuntimeError(
                                f"service_name already exists with different code_version and is still running: "
                                f"{effective_service_name}; existing={existing_code_version}; incoming={effective_code_version}; "
                                "stop the active service first, then redeploy with the same service_name"
                            )
                        if cached_session is None:
                            raise RuntimeError(
                                f"service_name already exists with different code_version but no local token cache was found: "
                                f"{effective_service_name}; existing={existing_code_version}; incoming={effective_code_version}"
                            )
                        try:
                            session_cache_lock = _acquire_service_session_lock_with_retry(
                                session_cache_file,
                                timeout_sec=timeout_sec,
                                action=(
                                    "another local deploy process is already active for "
                                    f"owner_client_id={effective_owner_client_id!r} service_name={effective_service_name!r}"
                                ),
                            )
                        except RuntimeError as exc:
                            raise RuntimeError(str(exc)) from exc
                        try:
                            cls._end_existing_group(
                                owner_client_id=effective_owner_client_id,
                                cache_payload=cached_session,
                                active_routes=existing_infos,
                                timeout_sec=timeout_sec,
                                reason="replace service with new code_version",
                            )
                        except Exception:
                            session_cache_lock.close()
                            session_cache_lock = None
                            raise
                        session_cache_lock.clear()
                        _emit_owner_notice(
                            f"stopped existing service before replace service_name={effective_service_name} "
                            f"existing={existing_code_version} incoming={effective_code_version}"
                        )

        try:
            if session_cache_lock is None:
                try:
                    session_cache_lock = _acquire_service_session_lock_with_retry(
                        session_cache_file,
                        timeout_sec=timeout_sec,
                        action=(
                            "another local deploy process is already active for "
                            f"owner_client_id={effective_owner_client_id!r} service_name={effective_service_name!r}"
                        ),
                    )
                except RuntimeError as exc:
                    raise RuntimeError(str(exc)) from exc
            sessions: Dict[str, ServiceSessionClient] = {}
            clients: Dict[str, Any] = {}
            nodes: Dict[str, InfoCenterNode] = {}
            failures: Dict[str, str] = {}

            def _create_service_on_node(node: InfoCenterNode) -> Tuple[str, InfoCenterNode, Optional[Any], Optional[ServiceSessionClient], str]:
                node_key = _node_instance_key_from_node(node)
                try:
                    target = _node_control_target_for_node(node)
                    client = _new_node_control_client(target, timeout_sec=timeout_sec)
                except Exception as exc:
                    return node_key, node, None, None, repr(exc)
                node_worker_count = max(1, int(worker_count or 1))
                if int(getattr(node, "service_worker_available", 0) or 0) > 0:
                    node_worker_count = max(1, min(node_worker_count, int(getattr(node, "service_worker_available", 0) or 0)))
                try:
                    session = client.create_service_from_bytes(
                        owner_client_id=effective_owner_client_id,
                        service_name=effective_service_name,
                        blob=effective_blob,
                        runtime=runtime,
                        entry_module=effective_entry_module,
                        entry_callable=entry_callable,
                        package_format=effective_package_format,
                        export_mode=export_mode,
                        export_methods=export_methods,
                        deps=prepared_artifact.dependency_policy,
                        managed_global_names=managed_global_names,
                        initial_globals=initial_globals_values,
                        policy_id=normalized_policy_id,
                        worker_count=node_worker_count,
                        heartbeat_timeout_sec=heartbeat_timeout_sec,
                        idle_ttl_sec=idle_ttl_sec,
                        expose_http=expose_http,
                        chunk_size=chunk_size,
                        api_token=effective_api_token,
                    )
                except Exception as exc:
                    client.close()
                    return node_key, node, None, None, repr(exc)
                session.node_instance_id = node_key
                session.node_id = str(node.node_id or "")
                return node_key, node, client, session, ""

            dispatch_results = dispatch_create_requests(
                selected_nodes,
                create_one=_create_service_on_node,
                thread_name_prefix="service-deploy",
                describe_error=lambda node, exc: repr(exc),
            )

            first_failure: Tuple[str, str] = ("", "")
            for item in dispatch_results:
                if item.created is None:
                    node_key = _node_instance_key_from_node(item.node)
                    error_message = item.error_message
                    failures[node_key] = error_message
                    if not first_failure[0]:
                        first_failure = (node_key, error_message)
                    continue
                node_key, node, client, session, error_message = item.created
                if error_message:
                    failures[node_key] = error_message
                    if not first_failure[0]:
                        first_failure = (node_key, error_message)
                    continue
                if client is None or session is None:
                    continue
                sessions[node_key] = session
                clients[node_key] = client
                nodes[node_key] = node

            if failures and not allow_partial:
                cls._cleanup_created_services(sessions=sessions, clients=clients, reason="rollback deploy")
                node_key, message = first_failure
                raise RuntimeError(
                    f"deploy failed on node={node_key}: {message}"
                )

            if len(sessions) < required_success_nodes:
                cls._cleanup_created_services(sessions=sessions, clients=clients, reason="insufficient success nodes")
                _emit_owner_notice(
                    "deploy failed: insufficient success nodes "
                    f"service_name={effective_service_name} success={len(sessions)} "
                    f"required={required_success_nodes} failures={failures}"
                )
                raise RuntimeError(
                    f"deploy success nodes={len(sessions)} < min_success_nodes={required_success_nodes}; "
                    f"failures={failures}"
                )

            group = cls(
                owner_client_id=effective_owner_client_id,
                service_name=effective_service_name,
                sessions=sessions,
                nodes=nodes,
                failures=failures,
                breaker_enabled=bool(breaker_enabled),
                breaker_failure_threshold=max(1, int(breaker_failure_threshold)),
                breaker_cooldown_sec=max(0.1, float(breaker_cooldown_sec)),
                breaker_max_cooldown_sec=max(0.1, float(breaker_max_cooldown_sec)),
                _clients=clients,
                _session_cache_file=session_cache_file,
                _session_cache_lock=session_cache_lock,
                _artifact_code_version=effective_code_version,
                serialization_mode=effective_policy.resolved_mode,
                policy_id=normalized_policy_id,
                effective_policy=effective_policy,
            )
            group._configure_dynamic_compensation(
                {
                    "infocenter_target": infocenter_target,
                    "blob": effective_blob,
                    "runtime": runtime,
                    "entry_module": effective_entry_module,
                    "entry_callable": entry_callable,
                    "package_format": effective_package_format,
                    "export_mode": export_mode,
                    "export_methods": export_methods,
                    "deps": prepared_artifact.dependency_policy,
                    "managed_global_names": managed_global_names,
                    "initial_globals": dict(initial_globals_values),
                    "policy_id": normalized_policy_id,
                    "worker_count": worker_count,
                    "heartbeat_timeout_sec": heartbeat_timeout_sec,
                    "idle_ttl_sec": idle_ttl_sec,
                    "expose_http": expose_http,
                    "chunk_size": chunk_size,
                    "healthy_only": healthy_only,
                    "tags": list(tags or ()),
                    "node_ids": requested_node_ids,
                    "node_instance_ids": requested_node_instance_ids,
                    "node_count": compensation_target_count,
                    "node_limit": node_limit,
                    "timeout_sec": timeout_sec,
                    "api_token": effective_api_token,
                }
            )
            group._persist_session_cache()
            group._start_keepalive()
            if failures:
                _emit_owner_notice(
                    "deploy success with partial failures "
                    f"service_name={effective_service_name} "
                    f"routes={_format_route_summary(group.route_summary())} "
                    f"failures={failures}"
                )
            else:
                _emit_owner_notice(
                    f"deploy success service_name={effective_service_name} "
                    f"routes={_format_route_summary(group.route_summary())}"
                )
            return group
        except Exception:
            if session_cache_lock is not None:
                session_cache_lock.close()
            raise

    @staticmethod
    def _is_active_service_status(status: int) -> bool:
        return int(status or 0) in (
            pb2.SERVICE_STATUS_STARTING,
            pb2.SERVICE_STATUS_RUNNING,
            pb2.SERVICE_STATUS_DRAINING,
        )

    @staticmethod
    def _select_active_routes(routes: Sequence[InfoCenterServiceRoute]) -> List[InfoCenterServiceRoute]:
        return [
            route
            for route in routes
            if Service._is_active_service_status(route.status)
        ]

    @classmethod
    def _inspect_existing_routes(
        cls,
        *,
        active_routes: Sequence[InfoCenterServiceRoute],
        timeout_sec: float,
    ) -> List[Tuple[InfoCenterServiceRoute, pb2.ServiceStatusInfo]]:
        out: List[Tuple[InfoCenterServiceRoute, pb2.ServiceStatusInfo]] = []
        failures: Dict[str, str] = {}
        http_only_routes: Dict[str, str] = {}
        for route in active_routes:
            route_key = _node_instance_key_from_route(route)
            control_addr = str(getattr(route, "control_addr", "") or "").strip()
            if not control_addr:
                http_only_routes[route_key] = (
                    f"service_id={str(getattr(route, 'service_id', '') or '') or '-'} "
                    f"service_name={str(getattr(route, 'service_name', '') or '') or '-'} "
                    f"http_base_url={str(getattr(route, 'http_base_url', '') or '') or '-'}"
                )
                continue
            client = _node_control_client(control_addr, timeout_sec=timeout_sec)
            try:
                info = client.get_service_status(service_id=route.service_id)
                out.append((route, info))
            except Exception as exc:
                failures[route_key] = repr(exc)
            finally:
                client.close()
        if http_only_routes:
            raise RuntimeError(
                "service_name already exists as startup/http-only service route; "
                f"Service.deploy cannot inspect or reuse routes without control_addr: {http_only_routes}"
            )
        if failures:
            raise RuntimeError(f"failed to inspect existing active service routes: {failures}")
        return out

    @classmethod
    def _reuse_existing_group(
        cls,
        *,
        owner_client_id: str,
        service_name: str,
        artifact_code_version: str,
        cache_payload: Dict[str, object],
        active_routes: Sequence[Tuple[InfoCenterServiceRoute, pb2.ServiceStatusInfo]],
        discovered_node_map: Dict[str, InfoCenterNode],
        timeout_sec: float,
        breaker_enabled: bool,
        breaker_failure_threshold: int,
        breaker_cooldown_sec: float,
        breaker_max_cooldown_sec: float,
        session_cache_file: Path,
        session_cache_lock: _ServiceSessionFileLock,
        policy_id: str = "",
    ) -> "Service":
        cache_nodes = cache_payload.get("nodes")
        if not isinstance(cache_nodes, dict):
            raise RuntimeError("invalid local service session cache: nodes missing")

        sessions: Dict[str, ServiceSessionClient] = {}
        clients: Dict[str, Any] = {}
        nodes: Dict[str, InfoCenterNode] = {}

        try:
            for route, info in active_routes:
                route_key = _node_instance_key_from_route(route)
                node = discovered_node_map.get(route_key)
                if node is None:
                    raise RuntimeError(
                        f"existing service route is outside current discovery scope: node_instance_id={route_key}"
                    )

                cached_node = cache_nodes.get(route_key)
                if not isinstance(cached_node, dict):
                    raise RuntimeError(
                        f"local service session cache missing node entry for reuse: node_instance_id={route_key}"
                    )

                cached_service_id = str(cached_node.get("service_id", "")).strip()
                cached_token = str(cached_node.get("service_token", "")).strip()
                if cached_service_id != route.service_id:
                    raise RuntimeError(
                        f"local service session cache is stale for node_instance_id={route_key}: "
                        f"cached_service_id={cached_service_id} route_service_id={route.service_id}"
                    )
                if not cached_token:
                    raise RuntimeError(f"local service session cache missing token for node_instance_id={route_key}")

                client = _node_control_client(route.control_addr, timeout_sec=timeout_sec)
                try:
                    hb = client.heartbeat_service(
                        owner_client_id=owner_client_id,
                        service_id=route.service_id,
                        service_token=cached_token,
                        seq=0,
                    )
                except Exception:
                    client.close()
                    raise

                sessions[route_key] = ServiceSessionClient(
                    _client=client,
                    owner_client_id=owner_client_id,
                    service_id=route.service_id,
                    service_token=cached_token,
                    code_version=str(info.code_version or ""),
                    http_base_url=str(cached_node.get("http_base_url", "") or info.http_base_url or route.http_base_url),
                    heartbeat_timeout_sec=max(
                        1,
                        int(
                            cached_node.get("heartbeat_timeout_sec", 0)
                            or (max(1, int(hb.next_heartbeat_in_sec or 0)) * 2)
                            or 30
                        ),
                    ),
                    worker_count=max(1, int(cached_node.get("worker_count", 0) or info.worker_count or route.worker_count or 1)),
                    status=hb.status or info.status,
                    service_name=str(info.service_name or route.service_name or ""),
                    node_instance_id=str(route_key or ""),
                    node_id=str(node.node_id or route.node_id or ""),
                    created_at=_timestamp_to_datetime(info.created_at),
                    last_heartbeat_at=_timestamp_to_datetime(info.last_heartbeat_at),
                    lease_expire_at=_timestamp_to_datetime(info.lease_expire_at),
                )
                clients[route_key] = client
                nodes[route_key] = node
        except Exception:
            for client in clients.values():
                try:
                    client.close()
                except Exception:
                    pass
            try:
                session_cache_lock.close()
            except Exception:
                pass
            raise

        group = cls(
            owner_client_id=owner_client_id,
            service_name=service_name,
            sessions=sessions,
            nodes=nodes,
            failures={},
            breaker_enabled=bool(breaker_enabled),
            breaker_failure_threshold=max(1, int(breaker_failure_threshold)),
            breaker_cooldown_sec=max(0.1, float(breaker_cooldown_sec)),
            breaker_max_cooldown_sec=max(0.1, float(breaker_max_cooldown_sec)),
            _clients=clients,
            _session_cache_file=session_cache_file,
            _session_cache_lock=session_cache_lock,
            _artifact_code_version=artifact_code_version,
            policy_id=str(policy_id or "").strip().lower() or get_default_policy_id_for_binding("service_internal"),
        )
        group._persist_session_cache()
        group._start_keepalive()
        return group

    @classmethod
    def _end_existing_group(
        cls,
        *,
        owner_client_id: str,
        cache_payload: Dict[str, object],
        active_routes: Sequence[Tuple[InfoCenterServiceRoute, pb2.ServiceStatusInfo]],
        timeout_sec: float,
        reason: str,
    ) -> None:
        cache_nodes = cache_payload.get("nodes")
        if not isinstance(cache_nodes, dict):
            raise RuntimeError("invalid local service session cache: nodes missing")

        failures: Dict[str, str] = {}
        for route, _info in active_routes:
            route_key = _node_instance_key_from_route(route)
            cached_node = cache_nodes.get(route_key)
            if not isinstance(cached_node, dict):
                failures[route_key] = "missing cached node entry"
                continue
            cached_service_id = str(cached_node.get("service_id", "")).strip()
            cached_token = str(cached_node.get("service_token", "")).strip()
            if cached_service_id != route.service_id or not cached_token:
                failures[route_key] = "stale or missing cached token"
                continue

            client = _node_control_client(route.control_addr, timeout_sec=timeout_sec)
            try:
                client.end_service(
                    owner_client_id=owner_client_id,
                    service_id=route.service_id,
                    service_token=cached_token,
                    reason=reason,
                )
            except Exception as exc:
                failures[route_key] = repr(exc)
            finally:
                client.close()

        if failures:
            raise RuntimeError(f"failed to end existing active service before replace: {failures}")

    @staticmethod
    def _cleanup_created_services(
        *,
        sessions: Dict[str, ServiceSessionClient],
        clients: Dict[str, Any],
        reason: str,
    ) -> None:
        for session in sessions.values():
            try:
                session.end(reason)
            except Exception:
                pass
        for client in clients.values():
            try:
                client.close()
            except Exception:
                pass

    def _persist_session_cache(self) -> None:
        if self._session_cache_file is None or not self.sessions:
            return
        payload: Dict[str, object] = {
            "schema_version": _SERVICE_SESSION_SCHEMA_VERSION,
            "saved_at": datetime.now(timezone.utc).isoformat(),
            "owner_client_id": self.owner_client_id,
            "service_name": self.service_name,
            "artifact_code_version": self._artifact_code_version,
            "policy_id": str(self._policy_id or "").strip().lower() or get_default_policy_id_for_binding("service_internal"),
            "nodes": {},
        }
        nodes_payload: Dict[str, object] = {}
        for node_key, session in sorted(self.sessions.items()):
            node = self.nodes.get(node_key)
            control_addr = ""
            if node is not None:
                control_addr = node.control_addr
            elif node_key in self._clients:
                control_addr = self._clients[node_key].target
            nodes_payload[node_key] = {
                "node_id": str(node.node_id if node is not None else ""),
                "control_addr": control_addr,
                "service_id": session.service_id,
                "service_token": session.service_token,
                "http_base_url": session.http_base_url,
                "heartbeat_timeout_sec": int(session.heartbeat_timeout_sec),
                "worker_count": int(session.worker_count),
            }
        payload["nodes"] = nodes_payload
        if self._session_cache_lock is not None:
            self._session_cache_lock.write_json(payload)
        else:
            _write_private_json(self._session_cache_file, payload)

    def _clear_session_cache(self) -> None:
        if self._session_cache_file is None:
            return
        if self._session_cache_lock is not None:
            self._session_cache_lock.clear()
            self._delete_session_cache_on_close = True
            return
        try:
            self._session_cache_file.unlink()
        except FileNotFoundError:
            pass

    def __post_init__(self, policy_id: str = "") -> None:
        self._init_execution_session_state()
        self._policy_id = str(policy_id or "").strip().lower() or get_default_policy_id_for_binding("service_internal")
        if self.effective_policy is None:
            self.effective_policy = _service_effective_policy_for_nodes(
                list(self.nodes.values()),
                policy_id=self._policy_id,
                requested_mode=self.serialization_mode,
                context="service_owner",
            )
        self.serialization_mode = self.effective_policy.resolved_mode
        if self.breaker_max_cooldown_sec < self.breaker_cooldown_sec:
            self.breaker_max_cooldown_sec = self.breaker_cooldown_sec
        for node_id in self.sessions:
            self._breaker_states.setdefault(node_id, CandidateBreakerState())

    def _breaker_state_locked(self, node_id: str) -> CandidateBreakerState:
        state = self._breaker_states.get(node_id)
        if state is None:
            state = CandidateBreakerState()
            self._breaker_states[node_id] = state
        return state

    def _breaker_mark_success(self, node_id: str) -> None:
        if not self.breaker_enabled:
            return
        with self._route_lock:
            state = self._breaker_state_locked(node_id)
            mark_candidate_success(state)

    def _breaker_mark_failure(self, node_id: str, exc: Exception, *, route_failure: bool = False) -> None:
        if not self.breaker_enabled:
            return
        with self._route_lock:
            state = self._breaker_state_locked(node_id)
            mark_candidate_failure(
                state,
                failure_kind=classify_service_error(exc, route_failure=route_failure),
                error=exc,
                failure_threshold=self.breaker_failure_threshold,
                cooldown_sec=self.breaker_cooldown_sec,
                max_cooldown_sec=self.breaker_max_cooldown_sec,
            )

    def _breaker_candidate_state(self, node_id: str) -> Tuple[str, bool]:
        if not self.breaker_enabled:
            return "closed", True
        with self._route_lock:
            state = self._breaker_state_locked(node_id)
            return candidate_allowed(state)

    def _breaker_before_invoke(self, node_id: str) -> bool:
        if not self.breaker_enabled:
            return True
        with self._route_lock:
            state = self._breaker_state_locked(node_id)
            return before_probe(state)

    def breaker_snapshot(self) -> Dict[str, Dict[str, object]]:
        now = time.monotonic()
        out: Dict[str, Dict[str, object]] = {}
        with self._route_lock:
            for node_id, state in self._breaker_states.items():
                remain = max(0.0, state.disabled_until_monotonic - now) if state.state == "open" else 0.0
                out[node_id] = {
                    "state": state.state,
                    "consecutive_failures": state.consecutive_failures,
                    "open_count": state.open_count,
                    "cooldown_remaining_sec": round(remain, 3),
                    "probe_in_flight": state.probe_in_flight,
                    "last_error": state.last_error,
                }
        return out

    def put_object_from_file(
        self,
        file_path: str,
        *,
        format: str = "",
        chunk_size: int = OBJECT_CHUNK_SIZE_BYTES,
    ) -> DataRef:
        refs = [
            client.upload_object_from_file(
                file_path=file_path,
                format=format,
                chunk_size=chunk_size,
            )
            for client in self._clients.values()
        ]
        if not refs:
            raise RuntimeError("no node clients available for object upload")
        object_ids = {ref.object_id for ref in refs}
        formats = {ref.format for ref in refs}
        if len(object_ids) != 1 or len(formats) != 1:
            raise RuntimeError(f"inconsistent object upload across nodes: {refs}")
        return refs[0]

    def put_object_from_bytes(
        self,
        blob: bytes,
        *,
        format: str = "",
        chunk_size: int = OBJECT_CHUNK_SIZE_BYTES,
    ) -> DataRef:
        refs = [
            client.upload_object_from_bytes(
                blob=blob,
                format=format,
                chunk_size=chunk_size,
            )
            for client in self._clients.values()
        ]
        if not refs:
            raise RuntimeError("no node clients available for object upload")
        object_ids = {ref.object_id for ref in refs}
        formats = {ref.format for ref in refs}
        if len(object_ids) != 1 or len(formats) != 1:
            raise RuntimeError(f"inconsistent object upload across nodes: {refs}")
        return refs[0]

    def put_data(
        self,
        data: Any,
        *,
        format: str = "",
        object_format: str = "",
        chunk_size: int = OBJECT_CHUNK_SIZE_BYTES,
        serialization_mode: str = "",
    ) -> DataRef:
        """Upload data to the service object store and return a DataRef.

        ``object_format`` is the preferred explicit name for the object-store
        format hint. ``format`` remains accepted for compatibility.
        """
        effective_format = str(object_format or format or "")
        effective_serialization_mode = resolve_effective_serialization_mode(
            request_mode=serialization_mode,
            context="object_upload",
            frozen_mode=self.serialization_mode,
        )
        return _put_data_via_clients(
            list(self._clients.values()),
            data,
            format=effective_format,
            chunk_size=chunk_size,
            serialization_mode=effective_serialization_mode,
        )

    def put_dataframe(
        self,
        dataframe: Any,
        *,
        chunk_size: int = OBJECT_CHUNK_SIZE_BYTES,
        serialization_mode: str = "",
    ) -> DataRef:
        return self.put_data(dataframe, format="parquet", chunk_size=chunk_size, serialization_mode=serialization_mode)

    def put_ndarray(
        self,
        array: Any,
        *,
        chunk_size: int = OBJECT_CHUNK_SIZE_BYTES,
        serialization_mode: str = "",
    ) -> DataRef:
        return self.put_data(array, format="npy", chunk_size=chunk_size, serialization_mode=serialization_mode)

    def put_json(
        self,
        value: Any,
        *,
        chunk_size: int = OBJECT_CHUNK_SIZE_BYTES,
        serialization_mode: str = "",
    ) -> DataRef:
        return self.put_data(value, format="json", chunk_size=chunk_size, serialization_mode=serialization_mode)

    def update_globals(self, values: Dict[str, object]) -> str:
        with self._route_lock:
            sessions_snapshot = list(self.sessions.items())
            clients_snapshot = dict(self._clients)
        active_clients = [clients_snapshot[node_id] for node_id, _ in sessions_snapshot if node_id in clients_snapshot]
        failed_nodes: Dict[str, str] = {}
        update_targets: List[Tuple[str, ServiceSessionClient]] = []
        for node_id, session in sessions_snapshot:
            if getattr(session, "failed", False):
                failed_nodes[node_id] = str(getattr(session, "last_error", "") or "session failed")
                continue
            update_targets.append((node_id, session))

        def _update_batch(
            _node_id: str,
            session: ServiceSessionClient,
            prepared_values: Dict[str, object],
            values_struct: object,
            transport_values: Optional[pb2.TransportPayload],
        ) -> object:
            update_encoded = getattr(session, "update_globals_encoded", None)
            if callable(update_encoded):
                return update_encoded(
                    prepared_keys=sorted(str(key) for key in prepared_values.keys()),
                    values=values_struct,
                    transport_values=transport_values,
                )
            return session.update_globals_prepared(
                prepared_values,
                serialization_mode=self.serialization_mode,
                effective_policy=self.effective_policy,
            )

        digests, update_failures = update_managed_globals_across_replicas(
            upload_clients=active_clients,
            values=values,
            targets=update_targets,
            serialization_mode=self.serialization_mode,
            effective_policy=self.effective_policy,
            context="service_owner",
            thread_name_prefix="service-update-globals",
            update_batch=_update_batch,
            include_empty_digest=True,
        )
        failed_nodes.update(update_failures)

        for node_id, message in failed_nodes.items():
            with self._route_lock:
                self.failures[node_id] = message
                self.sessions.pop(node_id, None)
                self.nodes.pop(node_id, None)
                client = self._clients.pop(node_id, None)
            if client is not None:
                try:
                    client.close()
                except Exception:
                    pass

        if not digests:
            raise RuntimeError(f"update_globals failed on all nodes: {failed_nodes}")
        self.globals_digests = dict(digests)
        self._last_managed_globals = dict(values or {})
        unique = {digest for digest in digests.values() if str(digest).strip()}
        return next(iter(unique), "") if len(unique) == 1 else next(iter(digests.values()))

    def __enter__(self) -> "Service":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close(end_services=False)

    def node_ids(self) -> Sequence[str]:
        return [self.nodes[key].node_id if key in self.nodes else key for key in self.sessions.keys()]

    def node_instance_ids(self) -> Sequence[str]:
        return list(self.sessions.keys())

    def _resolve_node_key(self, node_ref: str) -> str:
        normalized = str(node_ref or "").strip()
        if not normalized:
            raise KeyError("node reference is required")
        if normalized in self.sessions:
            return normalized
        matched = [node_key for node_key, node in self.nodes.items() if str(node.node_id or "").strip() == normalized]
        if len(matched) == 1:
            return matched[0]
        if len(matched) > 1:
            raise KeyError(
                f"ambiguous node_id: {normalized}; multiple live node instances match. Please use node_instance_id instead."
            )
        raise KeyError(f"unknown node reference: {normalized}")

    def _start_keepalive(self, interval_sec: Optional[float] = None) -> None:
        super()._start_keepalive(interval_sec=interval_sec)

    def join(
        self,
        timeout: Optional[float] = None,
        *,
        poll_interval_sec: float = 1.0,
        end_services_on_interrupt: bool = True,
        end_reason: str = "owner interrupted",
        handle_sigterm: bool = True,
        graceful_timeout_sec: float = 10.0,
    ) -> None:
        def _close_for_interrupt(reason: str) -> None:
            if not end_services_on_interrupt:
                self._stop_keepalive()
                return
            close_done = threading.Event()

            def _end() -> None:
                try:
                    self.close(end_services=True, reason=reason)
                finally:
                    close_done.set()

            thread = threading.Thread(target=_end, name=f"service-join-close-{self.service_name}", daemon=True)
            thread.start()
            if not close_done.wait(timeout=max(0.0, float(graceful_timeout_sec))):
                self._stop_keepalive()

        signal_event = threading.Event()
        previous_handlers: Dict[object, object] = {}

        def _signal_handler(signum, _frame) -> None:
            del signum, _frame
            signal_event.set()

        def _install_signal_handlers() -> None:
            names = ["SIGINT"]
            if handle_sigterm:
                names.append("SIGTERM")
            for name in names:
                sig = getattr(signal, name, None)
                if sig is None:
                    continue
                try:
                    previous_handlers[sig] = signal.getsignal(sig)
                    signal.signal(sig, _signal_handler)
                except (ValueError, OSError):
                    previous_handlers.pop(sig, None)

        def _restore_signal_handlers() -> None:
            for sig, previous in previous_handlers.items():
                with contextlib.suppress(Exception):
                    if signal.getsignal(sig) == _signal_handler:
                        signal.signal(sig, previous)

        wait_sec = max(0.1, float(poll_interval_sec))
        deadline = None if timeout is None else time.monotonic() + max(0.0, float(timeout))
        _install_signal_handlers()
        try:
            while True:
                with self._hb_lock:
                    thread = self._hb_thread
                if thread is None or not thread.is_alive():
                    self._sync_failures_from_replicas()
                    if self.failures:
                        _emit_owner_notice(
                            f"owner keepalive stopped service_name={self.service_name} failures={self.failures}"
                        )
                    return
                if deadline is not None:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        return
                    current_wait = min(wait_sec, remaining)
                else:
                    current_wait = wait_sec
                thread.join(timeout=current_wait)
                if signal_event.is_set():
                    _close_for_interrupt(end_reason)
                    return
        except KeyboardInterrupt:
            _close_for_interrupt(end_reason)
            return
        finally:
            _restore_signal_handlers()

    def _stop_keepalive(self) -> None:
        super()._stop_keepalive()

    def status_map(self) -> Dict[str, pb2.ServiceStatusInfo]:
        out: Dict[str, pb2.ServiceStatusInfo] = {}
        for node_key, session in self.sessions.items():
            out[node_key] = session.get_status()
        return out

    def _effective_worker_count(self) -> int:
        total_workers = 0
        for session in self.sessions.values():
            alive_workers = 0
            with contextlib.suppress(Exception):
                info = session.get_status()
                alive_workers = max(0, int(getattr(info, "alive_workers", 0) or 0))
            worker_count = max(0, int(getattr(session, "worker_count", 0) or 0))
            total_workers += max(alive_workers, worker_count, 0)
        return max(1, total_workers)

    def _default_max_in_flight(self) -> int:
        return _scaled_default_max_in_flight(self._effective_worker_count())

    def _get_async_call_gate(self) -> asyncio.Semaphore:
        loop = asyncio.get_running_loop()
        capacity = max(1, int(self._default_max_in_flight()))
        if (
            self._async_call_gate is None
            or self._async_call_gate_loop is not loop
            or self._async_call_gate_capacity != capacity
        ):
            self._async_call_gate = asyncio.Semaphore(capacity)
            self._async_call_gate_loop = loop
            self._async_call_gate_capacity = capacity
        return self._async_call_gate

    def _get_async_call_executor(self) -> ThreadPoolExecutor:
        capacity = max(1, int(self._default_max_in_flight()))
        if (
            self._async_call_executor is None
            or self._async_call_executor_capacity != capacity
        ):
            if self._async_call_executor is not None:
                with contextlib.suppress(Exception):
                    self._async_call_executor.shutdown(wait=False, cancel_futures=True)
            self._async_call_executor = ThreadPoolExecutor(
                max_workers=capacity,
                thread_name_prefix="service-call",
            )
            self._async_call_executor_capacity = capacity
        return self._async_call_executor

    def call_on_node(
        self,
        node_id: str,
        method: str,
        payload: Dict[str, object],
        *,
        timeout_sec: float = 60.0,
    ) -> Dict[str, object]:
        node_key = self._resolve_node_key(node_id)
        session = self.sessions.get(node_key)
        if session is None:
            raise KeyError(f"unknown node reference: {node_id}")
        return session.call(
            method,
            payload,
            timeout_sec=timeout_sec,
            serialization_mode=self.serialization_mode,
            effective_policy=self.effective_policy,
        )

    def _select_node(self, *, strategy: str, refresh_status: bool, exclude: Optional[Set[str]] = None) -> str:
        excluded = exclude or set()
        normalized_strategy, profile = resolve_service_strategy(strategy)
        all_candidates = [nid for nid in sorted(self.sessions.keys()) if nid not in excluded]
        candidates = []
        state_rank: Dict[str, int] = {}
        for node_id in all_candidates:
            breaker_state, allowed = self._breaker_candidate_state(node_id)
            if not allowed:
                continue
            state_rank[node_id] = 0 if breaker_state == "closed" else 1
            candidates.append(node_id)
        if not candidates:
            raise RuntimeError("no available service node (all candidates may be open-circuit)")

        if normalized_strategy == "round_robin":
            probe_candidates = [node_id for node_id in candidates if state_rank.get(node_id, 0) > 0]
            ranked_candidates = sorted(probe_candidates or candidates, key=lambda node_id: node_id)
            with self._route_lock:
                idx = self._route_index % len(ranked_candidates)
                self._route_index += 1
            return ranked_candidates[idx]

        if normalized_strategy not in {"predicted_busy", "least_inflight", "service_latency_first"}:
            raise ValueError("strategy must be one of: predicted_busy, service_default, service_latency_first, least_inflight, round_robin")

        best_node_id = ""
        best_key: Optional[Tuple[object, ...]] = None
        scheduler_candidates = []
        for node_id in candidates:
            session = self.sessions[node_id]
            info: Optional[pb2.ServiceStatusInfo] = None
            if refresh_status:
                try:
                    info = session.get_status()
                except Exception:
                    continue
                if info.status != pb2.SERVICE_STATUS_RUNNING:
                    continue
            in_flight = int(info.in_flight if info is not None else 0)
            alive_workers = int(info.alive_workers if info is not None else session.worker_count)
            predicted_busy = float(in_flight) / float(max(1, alive_workers))
            scheduler_candidates.append(
                (
                    node_id,
                    state_rank.get(node_id, 0),
                    predicted_busy,
                    in_flight,
                    alive_workers,
                )
            )

        if normalized_strategy == "least_inflight":
            for node_id, state_score, _predicted_busy, in_flight, alive_workers in scheduler_candidates:
                key = (float(state_score), float(in_flight), float(-alive_workers), node_id)
                if best_key is None or key < best_key:
                    best_key = key
                    best_node_id = node_id
        else:
            with self._route_lock:
                rr = self._route_index
                self._route_index += 1
            scheduler_rows = []
            for node_id, state_score, predicted_busy, in_flight, alive_workers in scheduler_candidates:
                scheduler_rows.append(
                    SchedulerCandidate(
                        id=str(node_id),
                        kind="service",
                        node_id=str(node_id),
                        node_instance_id=str(node_id),
                        healthy=True,
                        schedulable=True,
                        drain=False,
                        breaker_state="closed",
                        predicted_busy=predicted_busy,
                        node_inflight=in_flight,
                        alive_workers=max(1, alive_workers),
                        worker_capacity=max(1, int(self.sessions[node_id].worker_count or alive_workers or 1)),
                        credit=1,
                        recent_failures=int(self._breaker_states.get(node_id, NodeCircuitState()).consecutive_failures or 0),
                        metadata={"state_rank": state_score},
                    )
                )
            state = SchedulerState(
                recent_submit_failures={
                    str(node_id): int(self._breaker_states.get(node_id, NodeCircuitState()).consecutive_failures or 0)
                    for node_id, *_rest in scheduler_candidates
                }
            )
            chosen = select_one_candidate(
                scheduler_rows,
                profile=profile or SERVICE_DEFAULT,
                state=state,
                round_robin_counter=rr,
            )
            best_node_id = str(chosen.id)

        if best_node_id:
            return best_node_id

        with self._route_lock:
            idx = self._route_index % len(candidates)
            self._route_index += 1
        return candidates[idx]

    def call_balanced(
        self,
        method: str,
        payload: Dict[str, object],
        *,
        timeout_sec: float = 60.0,
        strategy: str = "predicted_busy",
        refresh_status: bool = True,
        max_attempts: int = 0,
        serialization_mode: str = "",
    ) -> Tuple[str, Dict[str, object]]:
        if not self.sessions:
            raise RuntimeError("Service session has no active replicas")
        effective_serialization_mode = resolve_effective_serialization_mode(
            request_mode=serialization_mode,
            context="service_owner",
            frozen_mode=self.serialization_mode,
        )

        tries = max(1, int(max_attempts or len(self.sessions)))
        excluded: Set[str] = set()
        last_error: Optional[Exception] = None

        for _ in range(tries):
            node_id = self._select_node(strategy=strategy, refresh_status=refresh_status, exclude=excluded)
            excluded.add(node_id)
            if not self._breaker_before_invoke(node_id):
                continue
            try:
                call_kwargs = {
                    "timeout_sec": timeout_sec,
                }
                if str(effective_serialization_mode or "").strip() and effective_serialization_mode != "legacy_v1":
                    call_kwargs["serialization_mode"] = effective_serialization_mode
                if self.effective_policy is not None:
                    call_kwargs["effective_policy"] = self.effective_policy
                resp = self.sessions[node_id].call(
                    method,
                    payload,
                    **call_kwargs,
                )
                self._breaker_mark_success(node_id)
                return node_id, resp
            except Exception as exc:
                last_error = exc
                failure_kind = classify_service_error(exc)
                self._breaker_mark_failure(node_id, exc)
                if not should_failover(
                    failure_kind,
                    has_alternative_candidate=(len(self.sessions) - len(excluded)) > 0,
                ):
                    raise RuntimeError(str(exc)) from exc

        raise RuntimeError(f"call failed on all candidate nodes: {last_error}")

    async def acall_balanced(
        self,
        method: str,
        payload: Dict[str, object],
        *,
        timeout_sec: float = 60.0,
        strategy: str = "predicted_busy",
        refresh_status: bool = True,
        max_attempts: int = 0,
        serialization_mode: str = "",
    ) -> Tuple[str, Dict[str, object]]:
        """异步版本的 call_balanced。

        使用 asyncio 在线程池中执行同步 HTTP 调用，不阻塞事件循环。

        Args:
            method: 服务方法名
            payload: 调用参数
            timeout_sec: 超时时间
            strategy: 节点选择策略（"predicted_busy"、"least_inflight" 或 "round_robin"）
            refresh_status: 是否在选择节点前刷新状态
            max_attempts: 最大尝试次数
        Returns:
            Tuple[str, Dict[str, object]]: (节点 ID, 响应结果)

        Raises:
            RuntimeError: 所有节点都调用失败时
        """
        if not self.sessions:
            raise RuntimeError("Service session has no active replicas")
        effective_serialization_mode = resolve_effective_serialization_mode(
            request_mode=serialization_mode,
            context="service_owner",
            frozen_mode=self.serialization_mode,
        )

        tries = max(1, int(max_attempts or len(self.sessions)))
        excluded: Set[str] = set()
        last_error: Optional[Exception] = None

        loop = asyncio.get_running_loop()
        async with self._get_async_call_gate():
            for _ in range(tries):
                node_id = self._select_node(strategy=strategy, refresh_status=refresh_status, exclude=excluded)
                excluded.add(node_id)
                if not self._breaker_before_invoke(node_id):
                    continue
                try:
                    # 在线程池中执行同步调用，不阻塞事件循环
                    call_kwargs = {
                        "timeout_sec": timeout_sec,
                    }
                    if str(effective_serialization_mode or "").strip() and effective_serialization_mode != "legacy_v1":
                        call_kwargs["serialization_mode"] = effective_serialization_mode
                    resp = await loop.run_in_executor(
                        self._get_async_call_executor(),
                        lambda nid=node_id, kwargs=call_kwargs: self.sessions[nid].call(
                            method,
                            payload,
                            **kwargs,
                        ),
                    )
                    self._breaker_mark_success(node_id)
                    return node_id, resp
                except Exception as exc:
                    last_error = exc
                    failure_kind = classify_service_error(exc)
                    self._breaker_mark_failure(node_id, exc)
                    if not should_failover(
                        failure_kind,
                        has_alternative_candidate=(len(self.sessions) - len(excluded)) > 0,
                    ):
                        raise RuntimeError(str(exc)) from exc

        raise RuntimeError(f"call failed on all candidate nodes: {last_error}")

    async def acall_all(
        self,
        method: str,
        payloads: Union[List[Dict[str, object]], Dict[str, object]],
        *,
        timeout_sec: float = 60.0,
        max_concurrency: int = 100,
    ) -> List[Tuple[Optional[str], Optional[Dict[str, object]], Optional[Exception]]]:
        """并发调用所有节点。

        将 payload 同时发送到所有可用节点，返回所有结果。

        Args:
            method: 服务方法名
            payloads: 可以是单个 payload（发送给所有节点）或 payload 列表（与节点一一对应）
            timeout_sec: 单次调用超时时间
            max_concurrency: 最大并发数

        Returns:
            List[Tuple[节点ID, 响应, 异常]]：所有节点的结果列表
        """
        if not self.sessions:
            raise RuntimeError("Service session has no active replicas")

        nodes = list(self.sessions.keys())
        # 如果是单个 payload，复制给所有节点
        if isinstance(payloads, dict):
            payloads = [dict(payloads) for _ in nodes]
        elif isinstance(payloads, list):
            if len(payloads) != len(nodes):
                raise ValueError(f"payload list length ({len(payloads)}) must match node count ({len(nodes)})")
            payloads = [dict(payload) for payload in payloads]

        loop = asyncio.get_running_loop()
        semaphore = asyncio.Semaphore(max_concurrency)

        async def _call_single(node_id: str, payload: Dict[str, object]) -> Tuple[Optional[str], Optional[Dict[str, object]], Optional[Exception]]:
            async with semaphore:
                if not self._breaker_before_invoke(node_id):
                    return node_id, None, RuntimeError("circuit breaker open")
                try:
                    resp = await loop.run_in_executor(
                        self._get_async_call_executor(),
                        lambda nid=node_id: self.sessions[nid].call(
                            method,
                            payload,
                            timeout_sec=timeout_sec,
                            serialization_mode=self.serialization_mode,
                        ),
                    )
                    self._breaker_mark_success(node_id)
                    return node_id, resp, None
                except Exception as exc:
                    self._breaker_mark_failure(node_id, exc)
                    return node_id, None, exc

        tasks = [_call_single(node_id, payload) for node_id, payload in zip(nodes, payloads)]
        return await asyncio.gather(*tasks)

    def end(self, reason: str = "group end") -> Dict[str, Optional[pb2.EndServiceResponse]]:
        self._stop_keepalive()
        out: Dict[str, Optional[pb2.EndServiceResponse]] = {}
        for node_id, session in self.sessions.items():
            try:
                out[node_id] = session.end(reason)
            except Exception:
                out[node_id] = None
        if out and all(
            resp is not None and resp.ok and resp.accepted and resp.status == pb2.SERVICE_STATUS_STOPPED
            for resp in out.values()
        ):
            self._clear_session_cache()
        return out

    def close(self, *, end_services: bool = False, reason: str = "group close") -> None:
        """Close this owner handle.

        By default this only closes the local owner/client resources and leaves
        remote service replicas running. Pass ``end_services=True`` or call
        ``shutdown_services()`` to stop remote replicas as well.
        """
        if self._closed:
            return
        self._closed = True
        self._stop_keepalive()
        if self._async_call_executor is not None:
            with contextlib.suppress(Exception):
                self._async_call_executor.shutdown(wait=False, cancel_futures=True)
            self._async_call_executor = None
            self._async_call_executor_capacity = 0
        if end_services:
            self.end(reason=reason)
        for client in self._clients.values():
            try:
                client.close()
            except Exception:
                pass
        self._clients.clear()
        if self._session_cache_lock is not None:
            self._session_cache_lock.close()
            self._session_cache_lock = None
        if self._delete_session_cache_on_close and self._session_cache_file is not None:
            try:
                self._session_cache_file.unlink()
            except FileNotFoundError:
                pass
            self._delete_session_cache_on_close = False

    def close_handle(self) -> None:
        """Close only the local owner handle; remote service replicas keep running."""
        self.close(end_services=False)

    def shutdown_services(self, *, reason: str = "service shutdown") -> None:
        """Stop remote service replicas and close this owner handle."""
        self.close(end_services=True, reason=reason)

    def __getattr__(self, name: str):
        if name.startswith("_"):
            raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")
        if self._discovered_methods is None:
            self._ensure_methods_discovered()
        if self._discovered_methods is not None and name not in self._discovered_methods:
            raise AttributeError(
                f"'{type(self).__name__}' has no method '{name}'. "
                f"Available methods: {self._discovered_methods}"
            )
        return _CallProxy(
            method=name,
            group=self,
            timeout_sec=60.0,
            strategy="predicted_busy",
            refresh_status=True,
        )

    def _ensure_methods_discovered(self) -> None:
        if self._discovered_methods is not None:
            return
        if self.sessions:
            first_session = next(iter(self.sessions.values()))
            try:
                methods = first_session.list_methods(include_docs=True)
                self._discovered_methods = [m.method for m in methods]
                return
            except Exception:
                pass
        self._discovered_methods = []

    def list_methods(self) -> List[str]:
        self._ensure_methods_discovered()
        return list(self._discovered_methods or [])

    @property
    def methods(self) -> List[str]:
        self._ensure_methods_discovered()
        return list(self._discovered_methods or [])

    async def call(self, method: str, **kwargs) -> Dict[str, object]:
        """Asynchronously call a service method.

        Use ``call_sync(...)`` for the synchronous variant.
        """
        node_id, resp = await self.acall_balanced(method, kwargs)
        return _resolve_high_level_service_data(self, node_id=node_id, response=resp)

    async def call_async(self, method: str, **kwargs) -> Dict[str, object]:
        """Explicit alias for ``call(...)``."""
        return await self.call(method, **kwargs)

    def call_sync(self, method: str, **kwargs) -> Dict[str, object]:
        """Synchronously call a service method."""
        node_id, resp = self.call_balanced(method, kwargs)
        return _resolve_high_level_service_data(self, node_id=node_id, response=resp)

    def map_calls(
        self,
        method: str,
        payloads: Sequence[Dict[str, object]],
        *,
        timeout_sec: float = 30.0,
        strategy: str = "predicted_busy",
        refresh_status: bool = True,
        max_in_flight: Optional[int] = None,
        progress: ProgressOption = False,
        progress_interval_sec: float = 2.0,
    ) -> List[Optional[object]]:
        return [
            item.result if item.ok else None
            for item in _service_collect_item_calls(
                self,
                method=method,
                payloads=payloads,
                timeout_sec=timeout_sec,
                strategy=strategy,
                refresh_status=refresh_status,
                max_in_flight=max_in_flight,
                progress=progress,
                progress_interval_sec=progress_interval_sec,
            )
        ]

    async def amap_calls(
        self,
        method: str,
        payloads: Sequence[Dict[str, object]],
        *,
        timeout_sec: float = 30.0,
        strategy: str = "predicted_busy",
        refresh_status: bool = True,
        max_in_flight: Optional[int] = None,
        progress: ProgressOption = False,
        progress_interval_sec: float = 2.0,
    ) -> List[Optional[object]]:
        items = await _service_acollect_item_calls(
            self,
            method=method,
            payloads=payloads,
            timeout_sec=timeout_sec,
            strategy=strategy,
            refresh_status=refresh_status,
            max_in_flight=max_in_flight,
            progress=progress,
            progress_interval_sec=progress_interval_sec,
        )
        return [item.result if item.ok else None for item in items]

    def unordered_calls(
        self,
        method: str,
        payloads: Sequence[Dict[str, object]],
        *,
        timeout_sec: float = 30.0,
        strategy: str = "predicted_busy",
        refresh_status: bool = True,
        max_in_flight: Optional[int] = None,
        return_items: bool = False,
        progress: ProgressOption = False,
        progress_interval_sec: float = 2.0,
    ) -> Iterator[Union[Tuple[int, Optional[object]], ExecutionItem]]:
        for item in _service_iter_item_calls(
            self,
            method=method,
            payloads=payloads,
            timeout_sec=timeout_sec,
            strategy=strategy,
            refresh_status=refresh_status,
            max_in_flight=max_in_flight,
            progress=progress,
            progress_interval_sec=progress_interval_sec,
        ):
            yield item if return_items else (item.index, item.result if item.ok else None)

    async def aunordered_calls(
        self,
        method: str,
        payloads: Sequence[Dict[str, object]],
        *,
        timeout_sec: float = 30.0,
        strategy: str = "predicted_busy",
        refresh_status: bool = True,
        max_in_flight: Optional[int] = None,
        return_items: bool = False,
        progress: ProgressOption = False,
        progress_interval_sec: float = 2.0,
    ) -> AsyncIterator[Union[Tuple[int, Optional[object]], ExecutionItem]]:
        async for item in _service_aiter_item_calls(
            self,
            method=method,
            payloads=payloads,
            timeout_sec=timeout_sec,
            strategy=strategy,
            refresh_status=refresh_status,
            max_in_flight=max_in_flight,
            progress=progress,
            progress_interval_sec=progress_interval_sec,
        ):
            yield item if return_items else (item.index, item.result if item.ok else None)

    def iter_item_calls(
        self,
        method: str,
        payloads: Sequence[Dict[str, object]],
        *,
        timeout_sec: float = 30.0,
        strategy: str = "predicted_busy",
        refresh_status: bool = True,
        max_in_flight: Optional[int] = None,
        progress: ProgressOption = False,
        progress_interval_sec: float = 2.0,
    ) -> Iterator[ExecutionItem]:
        return _service_iter_item_calls(
            self,
            method=method,
            payloads=payloads,
            timeout_sec=timeout_sec,
            strategy=strategy,
            refresh_status=refresh_status,
            max_in_flight=max_in_flight,
            progress=progress,
            progress_interval_sec=progress_interval_sec,
        )

    async def aiter_item_calls(
        self,
        method: str,
        payloads: Sequence[Dict[str, object]],
        *,
        timeout_sec: float = 30.0,
        strategy: str = "predicted_busy",
        refresh_status: bool = True,
        max_in_flight: Optional[int] = None,
        progress: ProgressOption = False,
        progress_interval_sec: float = 2.0,
    ) -> AsyncIterator[ExecutionItem]:
        async for item in _service_aiter_item_calls(
            self,
            method=method,
            payloads=payloads,
            timeout_sec=timeout_sec,
            strategy=strategy,
            refresh_status=refresh_status,
            max_in_flight=max_in_flight,
            progress=progress,
            progress_interval_sec=progress_interval_sec,
        ):
            yield item

    def collect_item_calls(
        self,
        method: str,
        payloads: Sequence[Dict[str, object]],
        *,
        timeout_sec: float = 30.0,
        strategy: str = "predicted_busy",
        refresh_status: bool = True,
        max_in_flight: Optional[int] = None,
        progress: ProgressOption = False,
        progress_interval_sec: float = 2.0,
    ) -> List[ExecutionItem]:
        return _service_collect_item_calls(
            self,
            method=method,
            payloads=payloads,
            timeout_sec=timeout_sec,
            strategy=strategy,
            refresh_status=refresh_status,
            max_in_flight=max_in_flight,
            progress=progress,
            progress_interval_sec=progress_interval_sec,
        )

    async def acollect_item_calls(
        self,
        method: str,
        payloads: Sequence[Dict[str, object]],
        *,
        timeout_sec: float = 30.0,
        strategy: str = "predicted_busy",
        refresh_status: bool = True,
        max_in_flight: Optional[int] = None,
        progress: ProgressOption = False,
        progress_interval_sec: float = 2.0,
    ) -> List[ExecutionItem]:
        return await _service_acollect_item_calls(
            self,
            method=method,
            payloads=payloads,
            timeout_sec=timeout_sec,
            strategy=strategy,
            refresh_status=refresh_status,
            max_in_flight=max_in_flight,
            progress=progress,
            progress_interval_sec=progress_interval_sec,
        )

    async def call_all(self, method: str, **kwargs) -> List[Tuple[Optional[str], Optional[object], Optional[Exception]]]:
        results = await self.acall_all(method, kwargs)
        return _resolve_high_level_service_results(self, results=results)

    def __repr__(self) -> str:
        node_ids = list(self.sessions.keys()) if self.sessions else []
        methods = self.methods if self._discovered_methods is not None else ["<not discovered>"]
        return (
            f"<Service "
            f"service={self.service_name!r} "
            f"nodes={len(node_ids)} "
            f"serialization_mode={self.serialization_mode} "
            f"methods={methods[:3]}{'...' if len(methods) > 3 else ''}>"
        )


__all__ = [
    "_ServiceSessionFileLock",
    "_service_session_cache_file",
    "_load_service_session_cache",
    "Service",
]
