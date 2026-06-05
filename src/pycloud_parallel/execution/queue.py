from __future__ import annotations

"""Authoritative V1 queue implementation."""

import inspect
import logging
from pathlib import Path
import time
import uuid
from typing import Any, Dict, List, Optional, Sequence

from pycloud_parallel.controlplane.artifact import (
    Artifact,
    _default_entry_module_for_module,
    _normalize_artifact_input,
    _prepare_artifact,
    _resolve_package_format,
)
from pycloud_parallel.controlplane.effective_policy import EffectivePolicy, resolve_effective_policy
from pycloud_parallel.controlplane.policy_profile import (
    get_default_mode_for_binding,
    get_default_policy_id_for_binding,
    get_policy_profile,
)
from pycloud_parallel.controlplane.serialization import LOCAL_IPC_SERIALIZATION_MODE, convert_dict_to_arrow
from pycloud_parallel.execution.support import (
    _JOB_UPDATE_GLOBALS_AUTO,
    _default_job_auth_ttl_sec,
    _default_job_finalize_for_blob,
    _default_job_finalize_for_module,
    _default_job_handle_result_for_blob,
    _default_job_handle_result_for_module,
    _default_job_task_generator_for_blob,
    _default_job_task_generator_for_module,
    _default_job_update_globals_for_blob,
    _load_job_client_session_cache,
    _normalize_job_update_globals_arg,
    _prepare_code_blob,
    _prepare_job_blob_submit_fields,
    _prepare_job_submit_payload_for_call,
    _stage_job_submit_payload_for_transport,
    _write_job_client_session_cache,
)

logger = logging.getLogger(__name__)

_JOBQUEUE_BINDING_ID = "jobqueue_controlplane_transport"
_JOBQUEUE_TRANSPORT_MODE = get_default_mode_for_binding(_JOBQUEUE_BINDING_ID)
_JOBQUEUE_POLICY_ID = get_default_policy_id_for_binding(_JOBQUEUE_BINDING_ID)

def _decode_job_response_payload(response: Dict[str, object]) -> Dict[str, object]:
    body = dict(response or {})
    data = body.get("data")
    if "job" not in body and isinstance(data, dict) and "job" in data:
        body = dict(data)
    job = body.get("job")
    if isinstance(job, dict):
        body["job"] = convert_dict_to_arrow(job)
    return body


def _jobqueue_effective_policy() -> EffectivePolicy:
    return resolve_effective_policy(
        get_policy_profile(_JOBQUEUE_POLICY_ID),
        requested_mode=_JOBQUEUE_TRANSPORT_MODE,
        context="jobqueue_session",
    )


def _is_local_target(target: str) -> bool:
    return str(target or "").strip().lower() == "local"


def _job_module_import_root(module: Any) -> str:
    module_file = str(getattr(module, "__file__", "") or "").strip()
    if not module_file:
        return ""
    try:
        module_path = Path(module_file).resolve()
    except Exception:
        return ""
    module_name = str(getattr(module, "__name__", "") or "").strip()
    parts = [part for part in module_name.split(".") if part]
    if not parts:
        return str(module_path.parent)
    if module_path.name == "__init__.py":
        levels = len(parts) + 1
    else:
        levels = max(1, len(parts))
    root = module_path
    for _ in range(levels):
        root = root.parent
    return str(root)


def _job_local_uses_module_import(
    *,
    target: str,
    source: Any,
    artifact: Optional[Any],
    deps: Optional[Any],
    package_format: str,
    resource_paths: Optional[Sequence[Any]],
    task_resource_paths: Optional[Sequence[Any]],
) -> bool:
    if not _is_local_target(target):
        return False
    if not inspect.ismodule(source):
        return False
    if artifact is not None or deps is not None:
        return False
    if str(package_format or "").strip():
        return False
    if any(str(item or "").strip() for item in list(resource_paths or ())):
        return False
    if any(str(item or "").strip() for item in list(task_resource_paths or ())):
        return False
    return bool(_default_entry_module_for_module(source))


class QueueServiceClient:
    """Thin client for the single job-orchestrator service resolved via InfoCenter."""

    def __init__(
        self,
        target: str,
        *,
        client_id: str = "",
        auth_token: str = "",
        timeout_sec: float = 10.0,
        service_name: str = "job-orchestrator",
        task_serialization_mode: str = "",
    ) -> None:
        self.target = str(target or "").strip()
        self.client_id = str(client_id or "").strip()
        self._client_scope = self.client_id
        self.timeout_sec = max(0.1, float(timeout_sec))
        self.service_name = str(service_name or "job-orchestrator").strip() or "job-orchestrator"
        self._default_task_serialization_mode = str(task_serialization_mode or "").strip()
        self.effective_policy = _jobqueue_effective_policy()
        self.serialization_mode = LOCAL_IPC_SERIALIZATION_MODE if _is_local_target(self.target) else self.effective_policy.resolved_mode
        self._auth_ttl_sec = _default_job_auth_ttl_sec()
        self._recent_job_ids: List[str] = []

        explicit_auth_token = str(auth_token or "").strip()
        cached_session = None
        if not explicit_auth_token:
            cached_session = _load_job_client_session_cache(
                target=self.target,
                service_name=self.service_name,
                client_scope=self._client_scope,
            )
        if cached_session is not None:
            self.client_id = str(cached_session.get("client_id", "") or "").strip()
            self.auth_token = str(cached_session.get("auth_token", "") or "").strip()
            self._recent_job_ids = [
                str(job_id).strip()
                for job_id in list(cached_session.get("recent_job_ids") or [])
                if str(job_id).strip()
            ][:20]
        else:
            if not self.client_id:
                self.client_id = f"job-client-{uuid.uuid4().hex[:8]}"
            self.auth_token = explicit_auth_token or uuid.uuid4().hex

        from pycloud_parallel.execution.service_session import Service

        self._service = Service._connect_route(
            target=self.target,
            service_name=self.service_name,
            timeout_sec=self.timeout_sec,
            service_token=self.auth_token,
            route="local" if _is_local_target(self.target) else "discovery",
            protocol="http",
            serialization_mode=self.serialization_mode,
            validate_on_init=False,
            effective_policy_override=None if _is_local_target(self.target) else self.effective_policy,
            prepare_discovery_payload=False,
        )
        self._persist_local_session()

    def _refresh_effective_policy(self) -> EffectivePolicy:
        effective_policy = _jobqueue_effective_policy()
        self.effective_policy = effective_policy
        self.serialization_mode = LOCAL_IPC_SERIALIZATION_MODE if _is_local_target(self.target) else effective_policy.resolved_mode
        return effective_policy

    def close(self) -> None:
        self._service.close()

    def _call_job_orchestrator(
        self,
        *,
        effective_policy: EffectivePolicy,
        **call_kwargs,
    ) -> Dict[str, object]:
        service_name = str(call_kwargs.get("service_name", "") or "").strip()
        method = str(call_kwargs.get("method", "") or "").strip()
        if service_name != self.service_name:
            raise ValueError(
                f"JobQueue service client is bound to service_name={self.service_name!r}, got {service_name!r}"
            )
        if not method:
            raise ValueError("JobQueue route lookup requires method")
        self.effective_policy = effective_policy
        self.serialization_mode = LOCAL_IPC_SERIALIZATION_MODE if _is_local_target(self.target) else effective_policy.resolved_mode
        self._service._fixed_effective_policy = None if _is_local_target(self.target) else effective_policy  # noqa: SLF001
        self._service.effective_policy = effective_policy
        self._service.serialization_mode = self.serialization_mode
        payload = dict(call_kwargs.get("payload") or {})
        if getattr(self._service, "route", "") == "local" and self.auth_token:
            payload.setdefault("_service_token", self.auth_token)
        effective_serialization_mode = (
            LOCAL_IPC_SERIALIZATION_MODE
            if _is_local_target(self.target)
            else (
                str(call_kwargs.get("serialization_mode", "") or "").strip()
                or str(self.serialization_mode or "").strip()
                or _JOBQUEUE_TRANSPORT_MODE
            )
        )
        _node_key, response = self._service.call_balanced(
            method,
            payload,
            timeout_sec=max(0.1, float(call_kwargs.get("timeout_sec", self.timeout_sec) or self.timeout_sec)),
            strategy="predicted_busy",
            refresh_status=True,
            serialization_mode=effective_serialization_mode,
        )
        return response

    def _persist_local_session(self) -> None:
        try:
            _write_job_client_session_cache(
                target=self.target,
                service_name=self.service_name,
                client_scope=self._client_scope,
                client_id=self.client_id,
                auth_token=self.auth_token,
                ttl_sec=self._auth_ttl_sec,
                recent_job_ids=self._recent_job_ids,
            )
        except Exception:
            logger.debug("job client session cache persist failed", exc_info=True)

    def _record_job_id(self, job_id: str) -> None:
        normalized = str(job_id or "").strip()
        if not normalized:
            return
        self._recent_job_ids = [item for item in self._recent_job_ids if item != normalized]
        self._recent_job_ids.insert(0, normalized)
        self._recent_job_ids = self._recent_job_ids[:20]
        self._persist_local_session()

    def __enter__(self) -> "QueueServiceClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def __repr__(self) -> str:
        effective_policy_text = ""
        if self.effective_policy is not None:
            effective_policy_text = (
                f" effective_policy={self.effective_policy.policy_id}@v{self.effective_policy.version}"
            )
        return (
            f"<JobQueue service_name={self.service_name!r} "
            f"client_id={self.client_id!r} serialization_mode={self.serialization_mode}"
            f"{effective_policy_text}>"
        )

    def submit_job(self, payload: Dict[str, object]) -> Dict[str, object]:
        raw_payload = dict(payload or {})
        if any(str(raw_payload.get(field, "") or "").strip() for field in ("policy_id", "taskpool_policy_id")):
            raise ValueError(
                "job submit policy_id/taskpool_policy_id is not supported; "
                "policy is owned by startup node/deployment"
            )
        effective_policy = self._refresh_effective_policy()
        if _is_local_target(self.target):
            prepared_payload = dict(raw_payload)
        else:
            prepared_payload = _stage_job_submit_payload_for_transport(
                target=self.target,
                payload=raw_payload,
                timeout_sec=self.timeout_sec,
                serialization_mode=self.serialization_mode,
            )
        if self.client_id and not str(prepared_payload.get("client_id", "") or "").strip():
            prepared_payload["client_id"] = self.client_id
        if not _is_local_target(self.target):
            prepared_payload = _prepare_job_submit_payload_for_call(
                target=self.target,
                payload=prepared_payload,
                timeout_sec=self.timeout_sec,
                serialization_mode=self.serialization_mode,
                effective_policy=effective_policy,
            )
        call_kwargs = {
            "service_name": self.service_name,
            "method": "submit_job",
            "payload": prepared_payload,
            "timeout_sec": self.timeout_sec,
        }
        if str(self.serialization_mode or "").strip() and self.serialization_mode != "legacy_v1":
            call_kwargs["serialization_mode"] = self.serialization_mode
        resp = self._call_job_orchestrator(
            effective_policy=effective_policy,
            **call_kwargs,
        )
        resp = _decode_job_response_payload(resp)
        job = dict(resp.get("job") or {})
        self._record_job_id(str(job.get("job_id", "") or "").strip())
        return resp

    def _prepare_submit_source_payload(
        self,
        *,
        source: Any = None,
        artifact: Optional[Any] = None,
        deps: Optional[Any] = None,
        job_payload: Optional[Dict[str, object]] = None,
        runtime: str = "py3",
        entry_module: Any = "",
        entry_callable: Any = "run",
        package_format: str = "",
        resource_paths: Optional[Sequence[Any]] = None,
        task_resource_paths: Optional[Sequence[Any]] = None,
        task_serialization_mode: str = "",
        reset_pool: bool = False,
        update_globals: Any = _JOB_UPDATE_GLOBALS_AUTO,
        handle_result_callable: str = "",
        finalize_callable: str = "",
    ) -> Dict[str, object]:
        module_source = None
        if source is not None:
            if inspect.ismodule(source):
                module_source = source
            elif callable(source) and not isinstance(source, (bytes, bytearray, memoryview)):
                raise ValueError(
                    "JobQueue.submit(source=callable) is not supported; use module/path/bytes or artifact="
                )
        elif isinstance(artifact, Artifact) and artifact.source_kind == "module" and inspect.ismodule(artifact.source_value):
            module_source = artifact.source_value

        normalized_resource_paths = [item for item in list(resource_paths or ()) if str(item or "").strip()]
        normalized_task_resource_paths = [item for item in list(task_resource_paths or ()) if str(item or "").strip()]
        if normalized_resource_paths and module_source is None:
            raise ValueError("resource_paths requires a module source")
        if normalized_task_resource_paths and module_source is None:
            raise ValueError("task_resource_paths requires a module source")

        normalize_kwargs = dict(
            consumer_kind="job",
            artifact=artifact,
            deps=deps,
            runtime=runtime,
            entry_module=entry_module,
            entry_callable=entry_callable,
            package_format=package_format,
        )
        bundled_module_resource_paths: list[Any] = []
        if module_source is not None:
            seen_resource_keys: set[str] = set()
            for item in [*normalized_resource_paths, *normalized_task_resource_paths]:
                key = str(item)
                if key in seen_resource_keys:
                    continue
                seen_resource_keys.add(key)
                bundled_module_resource_paths.append(item)

        use_local_module_import = _job_local_uses_module_import(
            target=self.target,
            source=module_source,
            artifact=artifact,
            deps=deps,
            package_format=package_format,
            resource_paths=resource_paths,
            task_resource_paths=task_resource_paths,
        )
        if use_local_module_import and module_source is not None:
            local_entry_module = str(entry_module or _default_entry_module_for_module(module_source)).strip()
            local_entry_callable = str(entry_callable or "run").strip() or "run"
            normalized_update_globals = _normalize_job_update_globals_arg(
                update_globals,
                auto_default=getattr(module_source, "update_globals", None),
            )
            effective_task_generator_callable = _default_job_task_generator_for_module(module_source)
            effective_handle_result_callable = str(
                handle_result_callable or _default_job_handle_result_for_module(module_source) or ""
            ).strip()
            effective_finalize_callable = str(
                finalize_callable or _default_job_finalize_for_module(module_source) or ""
            ).strip()
            payload: Dict[str, object] = {
                "job_mode": "hooks",
                "source_mode": "module_import",
                "runtime": str(runtime or "py3"),
                "entry_module": local_entry_module,
                "entry_callable": local_entry_callable,
                "package_format": "module_import",
                "task_generator_callable": effective_task_generator_callable,
                "job_payload": dict(job_payload or {}),
                "timeout_sec": max(10.0, float(self.timeout_sec)),
                "dependency_allowlist": [],
            }
            source_root = _job_module_import_root(module_source)
            if source_root:
                payload["source_root"] = source_root
        else:
            if bundled_module_resource_paths and module_source is not None:
                module_blob, module_filename = _prepare_code_blob(
                    module=module_source,
                    resource_paths=bundled_module_resource_paths,
                )
                normalized_artifact = _normalize_artifact_input(
                    source=module_blob,
                    consumer_kind="job",
                    deps=deps,
                    runtime=runtime,
                    entry_module=_default_entry_module_for_module(module_source),
                    entry_callable=entry_callable,
                    package_format=_resolve_package_format(package_format, module_filename, default="py"),
                )
            elif source is not None:
                normalized_artifact = _normalize_artifact_input(source=source, **normalize_kwargs)
            else:
                normalized_artifact = _normalize_artifact_input(**normalize_kwargs)
            prepared_artifact = _prepare_artifact(normalized_artifact, consumer_kind="job")

            normalized_update_globals = _normalize_job_update_globals_arg(
                update_globals,
                auto_default=(
                    getattr(module_source, "update_globals", None)
                    if module_source is not None
                    else _default_job_update_globals_for_blob(
                        prepared_artifact.blob,
                        package_format=prepared_artifact.package_format,
                    )
                ),
            )
            effective_task_generator_callable = (
                _default_job_task_generator_for_module(module_source)
                if module_source is not None
                else _default_job_task_generator_for_blob(
                    prepared_artifact.blob,
                    package_format=prepared_artifact.package_format,
                )
            )
            effective_handle_result_callable = str(
                handle_result_callable
                or (
                    _default_job_handle_result_for_module(module_source)
                    if module_source is not None
                    else _default_job_handle_result_for_blob(
                        prepared_artifact.blob,
                        package_format=prepared_artifact.package_format,
                    )
                )
                or ""
            ).strip()
            effective_finalize_callable = str(
                finalize_callable
                or (
                    _default_job_finalize_for_module(module_source)
                    if module_source is not None
                    else _default_job_finalize_for_blob(
                        prepared_artifact.blob,
                        package_format=prepared_artifact.package_format,
                    )
                )
                or ""
            ).strip()

            payload = {
                "job_mode": "hooks",
                "runtime": str(prepared_artifact.runtime or "py3"),
                "entry_module": str(prepared_artifact.entry_module or "").strip(),
                "entry_callable": str(prepared_artifact.entry_callable or "run").strip() or "run",
                "package_format": str(prepared_artifact.package_format or "py").strip() or "py",
                "task_generator_callable": effective_task_generator_callable,
                "job_payload": dict(job_payload or {}),
                "timeout_sec": max(10.0, float(self.timeout_sec)),
                "dependency_allowlist": list(prepared_artifact.dependency_allowlist),
            }
            payload.update(
                _prepare_job_blob_submit_fields(
                    target=self.target,
                    blob=prepared_artifact.blob,
                    package_format=prepared_artifact.package_format,
                    runtime=str(prepared_artifact.runtime or "py3"),
                    timeout_sec=self.timeout_sec,
                )
            )
        if effective_handle_result_callable:
            payload["handle_result_callable"] = effective_handle_result_callable
        if effective_finalize_callable:
            payload["finalize_callable"] = effective_finalize_callable
        if normalized_update_globals is not None:
            payload["update_globals"] = normalized_update_globals
        if normalized_task_resource_paths:
            payload["task_resource_paths"] = [str(item) for item in normalized_task_resource_paths]
        requested_task_serialization_mode = str(task_serialization_mode or "").strip().lower() or str(
            self._default_task_serialization_mode or ""
        ).strip().lower()
        if requested_task_serialization_mode:
            payload["task_serialization_mode"] = requested_task_serialization_mode
        if reset_pool:
            payload["reset_pool"] = True
        if self.client_id:
            payload["client_id"] = self.client_id
        return payload

    def submit(
        self,
        *,
        source: Any = None,
        artifact: Optional[Any] = None,
        deps: Optional[Any] = None,
        job_payload: Optional[Dict[str, object]] = None,
        runtime: str = "py3",
        entry_module: Any = "",
        entry_callable: Any = "run",
        package_format: str = "",
        resource_paths: Optional[Sequence[Any]] = None,
        task_resource_paths: Optional[Sequence[Any]] = None,
        task_serialization_mode: str = "",
        reset_pool: bool = False,
        update_globals: Any = _JOB_UPDATE_GLOBALS_AUTO,
        handle_result_callable: str = "",
        finalize_callable: str = "",
    ) -> Dict[str, object]:
        """Submit a job from a product-facing code source.

        Default path: ``submit(source=my_job_module, ...)``.
        Advanced path: ``submit(artifact=Artifact(...), ...)``.
        """
        payload = self._prepare_submit_source_payload(
            source=source,
            artifact=artifact,
            deps=deps,
            job_payload=job_payload,
            runtime=runtime,
            entry_module=entry_module,
            entry_callable=entry_callable,
            package_format=package_format,
            resource_paths=resource_paths,
            task_resource_paths=task_resource_paths,
            task_serialization_mode=task_serialization_mode,
            reset_pool=reset_pool,
            update_globals=update_globals,
            handle_result_callable=handle_result_callable,
            finalize_callable=finalize_callable,
        )
        return self.submit_job(payload)

    def recent_job_ids(self) -> List[str]:
        return list(self._recent_job_ids)

    def get_job_status(self, job_id: str, *, include_details: bool = False) -> Dict[str, object]:
        normalized = str(job_id or "").strip()
        if not normalized:
            raise ValueError("JobQueue.get_job_status() requires job_id")
        effective_policy = self._refresh_effective_policy()
        call_kwargs = {
            "service_name": self.service_name,
            "method": "get_job_status",
            "payload": {"job_id": normalized, "include_details": bool(include_details)},
            "timeout_sec": self.timeout_sec,
        }
        if str(self.serialization_mode or "").strip() and self.serialization_mode != "legacy_v1":
            call_kwargs["serialization_mode"] = self.serialization_mode
        return _decode_job_response_payload(
            self._call_job_orchestrator(
                effective_policy=effective_policy,
                **call_kwargs,
            )
        )

    def cancel_job(self, job_id: str) -> Dict[str, object]:
        normalized = str(job_id or "").strip()
        if not normalized:
            raise ValueError("JobQueue.cancel_job() requires job_id")
        effective_policy = self._refresh_effective_policy()
        call_kwargs = {
            "service_name": self.service_name,
            "method": "cancel_job",
            "payload": {"job_id": normalized},
            "timeout_sec": self.timeout_sec,
        }
        if str(self.serialization_mode or "").strip() and self.serialization_mode != "legacy_v1":
            call_kwargs["serialization_mode"] = self.serialization_mode
        return _decode_job_response_payload(
            self._call_job_orchestrator(
                effective_policy=effective_policy,
                **call_kwargs,
            )
        )

    def wait_for_terminal(
        self,
        job_id: str,
        *,
        timeout_sec: float = 30.0,
        poll_interval_sec: float = 0.5,
        include_details: bool = True,
    ) -> Dict[str, object]:
        normalized = str(job_id or "").strip()
        if not normalized:
            raise ValueError("JobQueue.wait_for_terminal() requires job_id")
        deadline = time.time() + max(0.1, float(timeout_sec))
        while time.time() < deadline:
            payload = self.get_job_status(normalized, include_details=False)
            job = dict(payload.get("job") or {})
            status = str(job.get("status", "") or "")
            if status in {"SUCCEEDED", "FAILED", "CANCELLED"}:
                if include_details:
                    return self.get_job_status(normalized, include_details=True)
                return payload
            time.sleep(max(0.05, float(poll_interval_sec)))
        raise TimeoutError(f"job did not reach terminal state before timeout: {normalized}")

class JobQueue(QueueServiceClient):
    """V1 public queue client."""

    @classmethod
    def connect(cls, target: str, **kwargs: Any) -> "JobQueue":
        """Product-facing connect action for the V1 job queue."""
        return cls(target, **kwargs)


__all__ = ["QueueServiceClient", "JobQueue"]
