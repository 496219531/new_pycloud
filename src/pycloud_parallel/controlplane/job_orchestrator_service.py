from __future__ import annotations

"""Importable system service module for the built-in job orchestrator."""

from typing import Any, Dict, List, Optional, Tuple

from pycloud_parallel.controlplane.job_orchestrator import (
    DEFAULT_JOB_ORCHESTRATOR_SERVICE_NAME,
    JobOrchestratorModule,
)
from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2


_MODULES: Dict[str, JobOrchestratorModule] = {}
_DEFAULT_SERVICE_ID = ""


def _raw(body: Dict[str, object], *, status_code: int = 200) -> Dict[str, object]:
    return {
        "__pycloud_raw_response__": True,
        "__pycloud_status_code__": int(status_code),
        **dict(body or {}),
    }


def configure(
    *,
    service_id: str,
    service_name: str = DEFAULT_JOB_ORCHESTRATOR_SERVICE_NAME,
    queue_capacity: int = 4000,
    taskpool_policy_id: str = "",
    admin_token: str = "",
    render_job_detail_page=None,
) -> JobOrchestratorModule:
    global _DEFAULT_SERVICE_ID
    module = JobOrchestratorModule(
        service_name=service_name,
        queue_capacity=queue_capacity,
        taskpool_policy_id=taskpool_policy_id,
        admin_token=admin_token,
        render_job_detail_page=render_job_detail_page,
    )
    module.service_id = str(service_id or module.service_id).strip() or module.service_id
    _MODULES[module.service_id] = module
    _DEFAULT_SERVICE_ID = module.service_id
    return module


def apply_managed_globals(values: Dict[str, Any], **context: object) -> None:
    service_id = str(values.get("service_id") or context.get("service_id") or "").strip()
    existing = _MODULES.get(service_id)
    service_name = str(
        values.get("service_name")
        or getattr(existing, "service_name", "")
        or DEFAULT_JOB_ORCHESTRATOR_SERVICE_NAME
    ).strip()
    queue_capacity = max(1, int(values.get("queue_capacity", getattr(existing, "queue_capacity", 4000)) or 1))
    taskpool_policy_id = str(values.get("taskpool_policy_id") or getattr(existing, "taskpool_policy_id", "") or "").strip().lower()
    admin_token = str(values.get("admin_token") or getattr(existing, "admin_token", "") or "").strip()
    render_job_detail_page = values.get("render_job_detail_page", getattr(existing, "_render_job_detail_page", None))
    base_url = str(values.get("base_url", getattr(existing, "base_url", "")) or "").strip().rstrip("/")
    if existing is not None and (
        str(existing.taskpool_policy_id or "").strip().lower() != taskpool_policy_id
        or int(existing.queue_capacity or 0) != queue_capacity
    ):
        existing.close()
        _MODULES.pop(service_id, None)
        existing = None
    if existing is None:
        existing = configure(
            service_id=service_id,
            service_name=service_name,
            queue_capacity=queue_capacity,
            taskpool_policy_id=taskpool_policy_id,
            admin_token=admin_token,
            render_job_detail_page=render_job_detail_page,
        )
    else:
        existing.service_name = service_name
        existing.admin_token = admin_token
        existing._render_job_detail_page = render_job_detail_page  # noqa: SLF001
    existing.base_url = base_url


def business_module(service_id: str = "") -> JobOrchestratorModule:
    normalized = str(service_id or "").strip() or _DEFAULT_SERVICE_ID
    module = _MODULES.get(normalized)
    if module is None:
        raise RuntimeError("job-orchestrator service module is not configured")
    return module


def start(*, controlplane_target: str, base_url: str = "", service_id: str = "") -> None:
    module = business_module(service_id)
    module.base_url = str(base_url or module.base_url or "").strip().rstrip("/")
    module.start(controlplane_target=str(controlplane_target or "").strip())


def close(service_id: str = "") -> None:
    global _DEFAULT_SERVICE_ID
    normalized = str(service_id or "").strip()
    if normalized:
        module = _MODULES.pop(normalized, None)
        if module is not None:
            module.close()
        if _DEFAULT_SERVICE_ID == normalized:
            _DEFAULT_SERVICE_ID = next(iter(_MODULES.keys()), "")
        return
    for module in list(_MODULES.values()):
        module.close()
    _MODULES.clear()
    _DEFAULT_SERVICE_ID = ""


def _response(result: Tuple[int, Dict[str, object]]) -> Dict[str, object]:
    status, body = result
    return _raw(body, status_code=status)


def submit_job(
    _service_id: str = "",
    _service_token: str = "",
    _serialization_mode: str = "",
    **payload: object,
) -> Dict[str, object]:
    return _response(
        business_module(_service_id).submit_job(
            dict(payload or {}),
            _service_token,
            serialization_mode=_serialization_mode,
        )
    )


def get_job_status(
    job_id: str = "",
    include_details: bool = False,
    _service_id: str = "",
    **_payload: object,
) -> Dict[str, object]:
    return _response(
        business_module(_service_id).get_job_status(
            job_id,
            include_details=include_details,
        )
    )


def cancel_job(
    job_id: str = "",
    _service_id: str = "",
    _service_token: str = "",
    **_payload: object,
) -> Dict[str, object]:
    return _response(
        business_module(_service_id).cancel_job(
            job_id,
            token=_service_token,
        )
    )


def reorder_job(
    job_id: str = "",
    direction: str = "",
    _service_id: str = "",
    _service_token: str = "",
    **_payload: object,
) -> Dict[str, object]:
    return _response(
        business_module(_service_id).reorder_job(
            job_id,
            direction=direction,
            token=_service_token,
        )
    )


def service_status(_service_id: str = "") -> Dict[str, object]:
    module = business_module(_service_id)
    return _raw(
        {
            "ok": True,
            "service": {
                "service_id": module.service_id,
                "service_name": module.service_name,
                "status": int(pb2.SERVICE_STATUS_RUNNING),
                "status_text": pb2.ServiceStatus.Name(pb2.SERVICE_STATUS_RUNNING),
                "http_base_url": f"{module.base_url}/svc/{module.service_id}" if module.base_url else "",
                "methods": ["cancel_job", "get_job_status", "reorder_job", "submit_job"],
            },
            "queue": module.job_queue.summary(),
        }
    )


def extra_get(
    path_parts: List[str],
    query: Dict[str, List[str]],
    _service_id: str = "",
) -> Optional[Tuple[object, ...]]:
    module = business_module(_service_id)
    return module.extra_get(path_parts, query)
