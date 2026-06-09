from __future__ import annotations

"""Thin shared helpers for low-frequency service/taskpool creation flows."""

import inspect
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Any, Callable, Dict, Generic, List, Optional, Sequence, Tuple, TypeVar

from pycloud_parallel.controlplane.artifact import (
    ArtifactExports,
    _default_entry_module_for_module,
    _normalize_artifact_input,
    _prepare_artifact,
    _resolve_package_format,
)
from pycloud_parallel.execution.error_classifier import ErrorCategory, classify_error
from pycloud_parallel.execution.support import _prepare_code_blob

TNode = TypeVar("TNode")
TCreated = TypeVar("TCreated")
_DEFAULT_CREATE_DISPATCH_MAX_WORKERS = 32


def _create_dispatch_max_workers(node_count: int) -> int:
    raw = str(os.environ.get("PYCLOUD_DEPLOY_CREATE_MAX_WORKERS", "") or "").strip()
    try:
        configured = int(raw) if raw else _DEFAULT_CREATE_DISPATCH_MAX_WORKERS
    except ValueError:
        configured = _DEFAULT_CREATE_DISPATCH_MAX_WORKERS
    return max(1, min(max(1, int(node_count or 1)), max(1, configured)))


@dataclass(frozen=True)
class CreateDispatchResult(Generic[TNode, TCreated]):
    node: TNode
    created: Optional[TCreated]
    error_message: str = ""


RETRYABLE_REPLICA_CREATE_CATEGORIES = frozenset(
    {
        ErrorCategory.TRANSIENT_NETWORK,
        ErrorCategory.IDENTITY_MISMATCH,
    }
)


def classify_replica_create_failures(
    failures: Dict[str, str],
    *,
    resource_kind: str,
) -> Dict[str, ErrorCategory]:
    return {
        str(node_id): classify_error(message, resource_kind=resource_kind)
        for node_id, message in dict(failures or {}).items()
        if str(node_id)
    }


def should_retry_replica_create_failures(
    failures: Dict[str, str],
    *,
    success: int,
    required: int,
    resource_kind: str,
) -> bool:
    if int(success or 0) >= int(required or 0):
        return False
    categories = classify_replica_create_failures(failures, resource_kind=resource_kind)
    if not categories:
        return False
    return any(category in RETRYABLE_REPLICA_CREATE_CATEGORIES for category in categories.values())


def next_replica_create_interval(
    attempt: int,
    *,
    deadline_remaining_sec: float,
    base_sec: float = 0.25,
    max_sec: float = 1.0,
) -> float:
    remaining = max(0.0, float(deadline_remaining_sec or 0.0))
    if remaining <= 0.0:
        return 0.0
    normalized_attempt = max(1, int(attempt or 1))
    interval = min(max(0.05, float(max_sec or 1.0)), max(0.05, float(base_sec or 0.25)) * normalized_attempt)
    return min(interval, remaining)


def run_replica_create_recovery_loop(
    *,
    timeout_sec: float,
    should_continue: Callable[[], bool],
    attempt_once: Callable[[int], None],
    base_interval_sec: float = 0.25,
    max_interval_sec: float = 1.0,
) -> int:
    retry_deadline = time.monotonic() + max(0.0, float(timeout_sec or 0.0))
    if retry_deadline <= time.monotonic():
        return 0
    attempt = 0
    while should_continue() and time.monotonic() < retry_deadline:
        attempt += 1
        sleep_sec = next_replica_create_interval(
            attempt,
            deadline_remaining_sec=retry_deadline - time.monotonic(),
            base_sec=base_interval_sec,
            max_sec=max_interval_sec,
        )
        if sleep_sec > 0.0:
            time.sleep(sleep_sec)
        if not should_continue() or time.monotonic() >= retry_deadline:
            break
        attempt_once(attempt)
    return attempt


def prepare_deployment_artifact(
    *,
    consumer_kind: str,
    source: Any,
    artifact: Optional[Any],
    deps: Optional[Any],
    runtime: str,
    entry_module: Any,
    entry_callable: Any,
    package_format: str,
    managed_global_names: Optional[Sequence[str]],
    export_methods: Optional[Sequence[str]] = None,
    resource_paths: Optional[Sequence[Any]] = None,
):
    module_source = source if inspect.ismodule(source) else None
    normalized_resource_paths = [item for item in list(resource_paths or ()) if str(item or "").strip()]
    effective_source = source
    effective_entry_module = entry_module
    effective_package_format = package_format
    if normalized_resource_paths and module_source is None:
        raise ValueError("resource_paths requires a module source")
    if normalized_resource_paths and module_source is not None:
        module_blob, module_filename = _prepare_code_blob(module=module_source, resource_paths=normalized_resource_paths)
        effective_source = module_blob
        effective_entry_module = _default_entry_module_for_module(module_source)
        effective_package_format = _resolve_package_format(package_format, module_filename, default="py")

    normalized_artifact = _normalize_artifact_input(
        consumer_kind=consumer_kind,
        source=effective_source,
        artifact=artifact,
        deps=deps,
        runtime=runtime,
        entry_module=effective_entry_module,
        entry_callable=entry_callable,
        package_format=effective_package_format,
        exports=ArtifactExports.explicit(export_methods) if export_methods else None,
        managed_global_names=managed_global_names,
    )
    return _prepare_artifact(normalized_artifact, consumer_kind=consumer_kind)


def normalize_initial_globals(
    initial_globals: Optional[Dict[str, object]],
    managed_global_names: Optional[Sequence[str]],
) -> Tuple[Dict[str, object], Tuple[str, ...]]:
    values = dict(initial_globals or {})
    names = [str(name).strip() for name in (managed_global_names or ()) if str(name).strip()]
    if values:
        known = set(names)
        for name in values:
            normalized_name = str(name).strip()
            if normalized_name and normalized_name not in known:
                names.append(normalized_name)
                known.add(normalized_name)
    return values, tuple(names)


def dispatch_create_requests(
    nodes: Sequence[TNode],
    *,
    create_one: Callable[[TNode], TCreated],
    thread_name_prefix: str,
    describe_error: Optional[Callable[[TNode, Exception], str]] = None,
) -> List[CreateDispatchResult[TNode, TCreated]]:
    def _run_create(node: TNode) -> CreateDispatchResult[TNode, TCreated]:
        try:
            return CreateDispatchResult(node=node, created=create_one(node), error_message="")
        except Exception as exc:
            return CreateDispatchResult(
                node=node,
                created=None,
                error_message=str(describe_error(node, exc) if describe_error is not None else repr(exc)),
            )

    normalized_nodes = list(nodes)
    if len(normalized_nodes) <= 1:
        return [_run_create(node) for node in normalized_nodes]

    results: List[CreateDispatchResult[TNode, TCreated]] = []
    with ThreadPoolExecutor(
        max_workers=_create_dispatch_max_workers(len(normalized_nodes)),
        thread_name_prefix=thread_name_prefix,
    ) as executor:
        futures = {executor.submit(_run_create, node): node for node in normalized_nodes}
        for future in as_completed(futures):
            result = future.result()
            results.append(result)
    ordered_index = {id(node): index for index, node in enumerate(normalized_nodes)}
    results.sort(key=lambda item: ordered_index.get(id(item.node), len(ordered_index)))
    return results


__all__ = [
    "CreateDispatchResult",
    "classify_replica_create_failures",
    "dispatch_create_requests",
    "normalize_initial_globals",
    "next_replica_create_interval",
    "prepare_deployment_artifact",
    "run_replica_create_recovery_loop",
    "should_retry_replica_create_failures",
]
