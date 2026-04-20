from __future__ import annotations

"""Additive helper layer for delayed job staging and staged DataRef lifecycle."""

from typing import Any, Dict, Iterable, List

from pycloud_parallel.controlplane.data_registry import DataRegistryClient
from pycloud_parallel.controlplane.job_queue import (
    _collect_payload_data_ref_ids,
    _resolve_payload_data_refs,
)
from pycloud_parallel.execution.support import _stage_job_submit_value


def stage_job_value(
    *,
    target: str,
    value: Any,
    runtime: str = "py3",
    timeout_sec: float = 10.0,
    replica_count: int = 2,
    ttl_sec: int = 24 * 60 * 60,
) -> Any:
    return _stage_job_submit_value(
        target=target,
        value=value,
        runtime=runtime,
        timeout_sec=timeout_sec,
        replica_count=replica_count,
        ttl_sec=ttl_sec,
    )


def collect_payload_data_refs(value: object) -> List[str]:
    return list(_collect_payload_data_ref_ids(value))


def touch_job_staged_refs(*, target: str, ref_ids: Iterable[str], timeout_sec: float = 10.0) -> List[str]:
    client = DataRegistryClient(target, timeout_sec=timeout_sec)
    touched: List[str] = []
    for ref_id in list(ref_ids or ()):
        normalized = str(ref_id or "").strip()
        if not normalized:
            continue
        client.touch(normalized)
        touched.append(normalized)
    return touched


def release_job_staged_refs(*, target: str, ref_ids: Iterable[str], timeout_sec: float = 10.0) -> List[str]:
    client = DataRegistryClient(target, timeout_sec=timeout_sec)
    released: List[str] = []
    for ref_id in list(ref_ids or ()):
        normalized = str(ref_id or "").strip()
        if not normalized:
            continue
        client.release(normalized)
        released.append(normalized)
    return released


def resolve_staged_payload(
    value: object,
    *,
    registry_target: str,
    timeout_sec: float = 10.0,
) -> object:
    return _resolve_payload_data_refs(
        value,
        registry_target=registry_target,
        timeout_sec=timeout_sec,
    )


__all__ = [
    "collect_payload_data_refs",
    "release_job_staged_refs",
    "resolve_staged_payload",
    "stage_job_value",
    "touch_job_staged_refs",
]
