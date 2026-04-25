from __future__ import annotations

"""Authoritative V1 queue implementation."""

import inspect
import logging
import time
import uuid
from typing import Any, Dict, List, Optional, Sequence, Set

from pycloud_parallel.controlplane.artifact import (
    Artifact,
    _default_entry_module_for_module,
    _normalize_artifact_input,
    _prepare_artifact,
    _resolve_package_format,
)
from pycloud_parallel.controlplane import client_transport as _client_transport
from pycloud_parallel.controlplane.effective_policy import EffectivePolicy, resolve_effective_policy
from pycloud_parallel.controlplane.infocenter_client import _route_sort_key
from pycloud_parallel.controlplane.policy_profile import (
    get_default_mode_for_binding,
    get_default_policy_id_for_binding,
    get_policy_profile,
)
from pycloud_parallel.controlplane.serialization import convert_dict_to_arrow
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
from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2

logger = logging.getLogger(__name__)

_JOBQUEUE_BINDING_ID = "jobqueue_controlplane_transport"
_JOBQUEUE_TRANSPORT_MODE = get_default_mode_for_binding(_JOBQUEUE_BINDING_ID)
_JOBQUEUE_POLICY_ID = get_default_policy_id_for_binding(_JOBQUEUE_BINDING_ID)

DiscoveryCallError = _client_transport.DiscoveryCallError
_call_route_http = _client_transport._call_route_http
_is_route_failure = _client_transport._is_route_failure


def _infocenter_client(*args, **kwargs):
    from pycloud_parallel.controlplane.infocenter_client import InfoCenterClient

    return InfoCenterClient(*args, **kwargs)


def _decode_job_response_payload(response: Dict[str, object]) -> Dict[str, object]:
    body = dict(response or {})
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


class _JobOrchestratorDiscoveryClient:
    """Resolve job-orchestrator via InfoCenter and call its HTTP endpoint directly."""

    def __init__(
        self,
        target: str,
        *,
        timeout_sec: float = 10.0,
        service_token: str = "",
        serialization_mode: str = "",
        effective_policy: Optional[EffectivePolicy] = None,
    ) -> None:
        del serialization_mode
        self.target = str(target or "").strip()
        self.timeout_sec = max(0.1, float(timeout_sec))
        self.service_token = str(service_token or "").strip()
        self.effective_policy = effective_policy or _jobqueue_effective_policy()
        self.serialization_mode = str(self.effective_policy.resolved_mode or _JOBQUEUE_TRANSPORT_MODE).strip() or _JOBQUEUE_TRANSPORT_MODE

    def close(self) -> None:
        return None

    def _list_routes(self, *, service_name: str) -> List[object]:
        try:
            with _infocenter_client(self.target, timeout_sec=self.timeout_sec) as client:
                routes = list(
                    client.list_service_routes(
                        service_name=str(service_name or "").strip(),
                        healthy_only=True,
                        limit=32,
                    )
                )
        except Exception as exc:
            raise RuntimeError(f"failed to query service routes from InfoCenter target={self.target}: {exc}") from exc
        candidates = [
            route
            for route in routes
            if route.status == pb2.SERVICE_STATUS_RUNNING and str(route.http_base_url or "").strip()
        ]
        candidates.sort(key=lambda route: _route_sort_key(route, strategy="predicted_busy"))
        return candidates

    def _resolve_effective_policy_for_routes(
        self,
        routes: Sequence[object],
        *,
        requested_mode: str = "",
    ) -> EffectivePolicy:
        del routes, requested_mode
        self.effective_policy = _jobqueue_effective_policy()
        self.serialization_mode = self.effective_policy.resolved_mode
        return self.effective_policy

    def resolve_effective_policy_for_service(
        self,
        *,
        service_name: str,
        requested_mode: str = "",
    ) -> EffectivePolicy:
        del service_name, requested_mode
        return _jobqueue_effective_policy()

    def call(
        self,
        *,
        service_name: str,
        method: str,
        payload: Optional[Dict[str, object]] = None,
        timeout_sec: float = 60.0,
        service_token: Optional[str] = None,
        serialization_mode: str = "",
        effective_policy: Optional[EffectivePolicy] = None,
    ) -> Dict[str, object]:
        name = str(service_name or "").strip()
        method_name = str(method or "").strip()
        if not name:
            raise ValueError("JobQueue route lookup requires service_name")
        if not method_name:
            raise ValueError("JobQueue route lookup requires method")

        token = self.service_token if service_token is None else str(service_token or "").strip()
        tried: Set[str] = set()
        routes = self._list_routes(service_name=name)
        if not routes:
            raise RuntimeError(
                f"JobQueue could not find a running job-orchestrator route for service_name={name!r}"
            )
        del serialization_mode
        route_effective_policy = _jobqueue_effective_policy()
        effective_serialization_mode = route_effective_policy.resolved_mode

        last_exc: Optional[Exception] = None
        for route in routes:
            if route.service_id in tried:
                continue
            tried.add(route.service_id)
            try:
                call_kwargs = {
                    "method": method_name,
                    "payload": payload or {},
                    "timeout_sec": max(0.1, float(timeout_sec)),
                    "service_token": token,
                }
                if (
                    str(effective_serialization_mode or "").strip()
                    and effective_serialization_mode != "legacy_v1"
                ):
                    call_kwargs["serialization_mode"] = effective_serialization_mode
                call_kwargs["effective_policy"] = route_effective_policy
                return _call_route_http(
                    route,
                    **call_kwargs,
                )
            except DiscoveryCallError as exc:
                last_exc = exc
                if not _is_route_failure(exc):
                    raise RuntimeError(str(exc)) from exc
                continue

        if last_exc is not None:
            raise RuntimeError(str(last_exc)) from last_exc
        raise RuntimeError(
            f"JobQueue could not find a usable job-orchestrator route for service_name={name!r}"
        )


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
        self.serialization_mode = self.effective_policy.resolved_mode
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

        self._service_client = _JobOrchestratorDiscoveryClient(
            self.target,
            timeout_sec=self.timeout_sec,
            service_token=self.auth_token,
            serialization_mode=self.serialization_mode,
            effective_policy=self.effective_policy,
        )
        self._persist_local_session()

    def _refresh_effective_policy(self) -> EffectivePolicy:
        effective_policy = _jobqueue_effective_policy()
        self.effective_policy = effective_policy
        self.serialization_mode = effective_policy.resolved_mode
        return effective_policy

    def close(self) -> None:
        self._service_client.close()

    def _call_service_client(
        self,
        *,
        effective_policy: EffectivePolicy,
        **call_kwargs,
    ) -> Dict[str, object]:
        try:
            return self._service_client.call(
                effective_policy=effective_policy,
                **call_kwargs,
            )
        except TypeError as exc:
            message = str(exc)
            if "effective_policy" not in message and "serialization_mode" not in message:
                raise
            fallback_kwargs = dict(call_kwargs)
            fallback_kwargs.pop("serialization_mode", None)
            return self._service_client.call(**fallback_kwargs)

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
        effective_policy = self._refresh_effective_policy()
        prepared_payload = _stage_job_submit_payload_for_transport(
            target=self.target,
            payload=dict(payload or {}),
            timeout_sec=self.timeout_sec,
            serialization_mode=self.serialization_mode,
        )
        if self.client_id and not str(prepared_payload.get("client_id", "") or "").strip():
            prepared_payload["client_id"] = self.client_id
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
        resp = self._call_service_client(
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
        dependency_allowlist: Optional[Sequence[str]] = None,
        resource_paths: Optional[Sequence[Any]] = None,
        task_resource_paths: Optional[Sequence[Any]] = None,
        task_serialization_mode: str = "",
        policy_id: str = "",
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
            dependency_allowlist=dependency_allowlist,
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
                dependency_allowlist=dependency_allowlist,
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

        payload: Dict[str, object] = {
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
        if effective_handle_result_callable:
            payload["handle_result_callable"] = effective_handle_result_callable
        if effective_finalize_callable:
            payload["finalize_callable"] = effective_finalize_callable
        payload.update(
            _prepare_job_blob_submit_fields(
                target=self.target,
                blob=prepared_artifact.blob,
                package_format=prepared_artifact.package_format,
                runtime=str(prepared_artifact.runtime or "py3"),
                timeout_sec=self.timeout_sec,
            )
        )
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
        dependency_allowlist: Optional[Sequence[str]] = None,
        resource_paths: Optional[Sequence[Any]] = None,
        task_resource_paths: Optional[Sequence[Any]] = None,
        task_serialization_mode: str = "",
        policy_id: str = "",
        reset_pool: bool = False,
        update_globals: Any = _JOB_UPDATE_GLOBALS_AUTO,
        handle_result_callable: str = "",
        finalize_callable: str = "",
    ) -> Dict[str, object]:
        """Submit a job from a product-facing code source.

        Default path: ``submit(source=my_job_module, ...)``.
        Advanced path: ``submit(artifact=Artifact(...), ...)``.
        """
        if str(policy_id or "").strip():
            raise ValueError("JobQueue.submit() no longer accepts policy_id; only task_serialization_mode is allowed")
        payload = self._prepare_submit_source_payload(
            source=source,
            artifact=artifact,
            deps=deps,
            job_payload=job_payload,
            runtime=runtime,
            entry_module=entry_module,
            entry_callable=entry_callable,
            package_format=package_format,
            dependency_allowlist=dependency_allowlist,
            resource_paths=resource_paths,
            task_resource_paths=task_resource_paths,
            task_serialization_mode=task_serialization_mode,
            policy_id=policy_id,
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
            self._call_service_client(
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
            self._call_service_client(
                effective_policy=effective_policy,
                **call_kwargs,
            )
        )

    def submit_job_from_bytes(
        self,
        *,
        blob: bytes,
        entry_module: str,
        job_payload: Optional[Dict[str, object]] = None,
        runtime: str = "py3",
        package_format: str = "py",
        dependency_allowlist: Optional[Sequence[str]] = None,
        task_serialization_mode: str = "",
        policy_id: str = "",
        reset_pool: bool = False,
        update_globals: Any = _JOB_UPDATE_GLOBALS_AUTO,
        handle_result_callable: str = "",
        finalize_callable: str = "",
    ) -> Dict[str, object]:
        return self.submit(
            source=blob,
            job_payload=job_payload,
            runtime=runtime,
            entry_module=entry_module,
            entry_callable="run",
            package_format=_resolve_package_format(package_format, default="py"),
            dependency_allowlist=dependency_allowlist,
            task_serialization_mode=task_serialization_mode,
            policy_id=policy_id,
            reset_pool=reset_pool,
            update_globals=update_globals,
            handle_result_callable=handle_result_callable,
            finalize_callable=finalize_callable,
        )

    def submit_job_from_module(
        self,
        *,
        module: Any,
        job_payload: Optional[Dict[str, object]] = None,
        runtime: str = "py3",
        dependency_allowlist: Optional[Sequence[str]] = None,
        resource_paths: Optional[Sequence[Any]] = None,
        task_resource_paths: Optional[Sequence[Any]] = None,
        task_serialization_mode: str = "",
        policy_id: str = "",
        reset_pool: bool = False,
        update_globals: Any = _JOB_UPDATE_GLOBALS_AUTO,
        handle_result_callable: str = "",
        finalize_callable: str = "",
    ) -> Dict[str, object]:
        return self.submit(
            source=module,
            job_payload=job_payload,
            runtime=runtime,
            dependency_allowlist=dependency_allowlist,
            resource_paths=resource_paths,
            task_resource_paths=task_resource_paths,
            task_serialization_mode=task_serialization_mode,
            policy_id=policy_id,
            reset_pool=reset_pool,
            update_globals=update_globals,
            handle_result_callable=handle_result_callable,
            finalize_callable=finalize_callable,
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


__all__ = ["QueueServiceClient", "JobQueue", "_JobOrchestratorDiscoveryClient"]
