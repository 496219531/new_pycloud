from __future__ import annotations

"""Thin shared helpers for low-frequency service/taskpool creation flows."""

import inspect
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
from pycloud_parallel.execution.support import _prepare_code_blob

TNode = TypeVar("TNode")
TCreated = TypeVar("TCreated")


@dataclass(frozen=True)
class CreateDispatchResult(Generic[TNode, TCreated]):
    node: TNode
    created: Optional[TCreated]
    error_message: str = ""


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
    with ThreadPoolExecutor(max_workers=max(1, len(normalized_nodes)), thread_name_prefix=thread_name_prefix) as executor:
        futures = {executor.submit(_run_create, node): node for node in normalized_nodes}
        for future in as_completed(futures):
            result = future.result()
            results.append(result)
    ordered_index = {id(node): index for index, node in enumerate(normalized_nodes)}
    results.sort(key=lambda item: ordered_index.get(id(item.node), len(ordered_index)))
    return results


__all__ = [
    "CreateDispatchResult",
    "dispatch_create_requests",
    "normalize_initial_globals",
    "prepare_deployment_artifact",
]
