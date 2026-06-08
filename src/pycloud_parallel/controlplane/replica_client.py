from __future__ import annotations

"""Replica-scoped client handles for service sessions and task pools."""

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, Optional, Sequence
from urllib.parse import quote, urlencode

from pycloud_parallel.controlplane.effective_policy import EffectivePolicy
from .client_transport import (
    _normalize_http_response_body,
    _serialize_http_call_payload,
)
from pycloud_parallel.controlplane.client_transport_runtime import RuntimeTransportRequest, runtime_http_request
from pycloud_parallel.data.ref import DataRef, maybe_data_ref
from pycloud_parallel.controlplane.remote_payload import prepare_remote_call_payload
from pycloud_parallel.controlplane.session_model import (
    ExecutionReplicaSnapshot,
    SessionBinding,
    SessionIdentity,
    SessionLease,
)
from pycloud_parallel.proto.v1 import pycloud_v1_pb2 as pb2


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _timestamp_to_datetime(value) -> datetime:
    if value is None:
        return _utc_now()
    try:
        dt = value.ToDatetime()
    except Exception:
        return _utc_now()
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _heartbeat_rpc_timeout_sec(heartbeat_timeout_sec: int) -> float:
    lease_sec = max(1.0, float(heartbeat_timeout_sec or 1))
    return max(0.5, min(5.0, lease_sec / 4.0))


def _extract_result_ref(value: object) -> Optional[DataRef]:
    direct = maybe_data_ref(value)
    if direct is not None:
        return direct
    if isinstance(value, dict):
        return maybe_data_ref(value.get("data"))
    return None


@dataclass
class NativeTaskPoolClient:
    kind: str = field(init=False, default="task_pool")
    _client: Any = field(repr=False)
    owner_client_id: str
    pool_id: str
    pool_token: str
    code_version: str
    worker_count: int
    heartbeat_timeout_sec: int = 30
    pool_name: str = ""
    idle_ttl_sec: int = 0
    node_instance_id: str = ""
    node_id: str = ""
    status: str = "RUNNING"
    created_at: Optional[datetime] = None
    last_heartbeat_at: Optional[datetime] = None
    lease_expire_at: Optional[datetime] = None
    failed: bool = False
    last_error: str = ""
    heartbeat_failure_threshold: int = 3

    @property
    def session_id(self) -> str:
        return str(self.pool_id or "")

    @property
    def session_name(self) -> str:
        return str(self.pool_name or self.pool_id or "")

    @property
    def session_token(self) -> str:
        return str(self.pool_token or "")

    def identity(self) -> SessionIdentity:
        return SessionIdentity(
            kind="task_pool",
            session_id=self.session_id,
            session_name=self.session_name,
            owner_client_id=str(self.owner_client_id or ""),
            session_token=self.session_token,
        )

    def lease(self) -> SessionLease:
        created_at = self.created_at or self.last_heartbeat_at or _utc_now()
        last_heartbeat_at = self.last_heartbeat_at or created_at
        lease_expire_at = self.lease_expire_at or (last_heartbeat_at + timedelta(seconds=max(1, int(self.heartbeat_timeout_sec or 0))))
        return SessionLease(
            heartbeat_timeout_sec=max(1, int(self.heartbeat_timeout_sec or 0)),
            idle_ttl_sec=max(0, int(self.idle_ttl_sec or 0)),
            created_at=created_at,
            last_heartbeat_at=last_heartbeat_at,
            lease_expire_at=lease_expire_at,
        )

    def binding(self) -> SessionBinding:
        return SessionBinding(
            code_version=str(self.code_version or ""),
            worker_count=max(0, int(self.worker_count or 0)),
        )

    def snapshot(
        self,
        *,
        node_instance_id: str = "",
        node_id: str = "",
        failure: str = "",
    ) -> ExecutionReplicaSnapshot:
        lease = self.lease()
        status_text = str(self.status or "RUNNING")
        failure_text = str(failure or self.last_error or "")
        alive = not bool(self.failed) and not failure_text.strip() and status_text.upper() == "RUNNING"
        return ExecutionReplicaSnapshot(
            kind="task_pool",
            node_instance_id=str(node_instance_id or self.node_instance_id or ""),
            node_id=str(node_id or self.node_id or ""),
            session_id=self.session_id,
            session_name=self.session_name,
            code_version=str(self.code_version or ""),
            worker_count=max(0, int(self.worker_count or 0)),
            alive=alive,
            status=status_text,
            lease_expire_at=lease.lease_expire_at,
            failure=failure_text,
        )

    def submit_tasks(
        self,
        tasks: Sequence[pb2.TaskSubmitItem],
        *,
        job_id: str = "",
    ) -> pb2.SubmitTasksResponse:
        return self._client.submit_pool_tasks(
            pool_id=self.pool_id,
            pool_token=self.pool_token,
            tasks=tasks,
            job_id=job_id,
        )

    def pull_results(
        self,
        *,
        limit: int = 100,
        wait_ms: int = 0,
        cursor: str = "",
    ) -> pb2.PullResultsResponse:
        return self._client.pull_pool_results(
            pool_id=self.pool_id,
            pool_token=self.pool_token,
            limit=limit,
            wait_ms=wait_ms,
            cursor=cursor,
        )

    def close(self, *, reason: str = "") -> pb2.CloseTaskPoolResponse:
        return self._client.close_task_pool(
            owner_client_id=self.owner_client_id,
            pool_id=self.pool_id,
            pool_token=self.pool_token,
            reason=reason,
        )

    def heartbeat(self, *, seq: int = 0) -> pb2.HeartbeatTaskPoolResponse:
        resp = self._client.heartbeat_task_pool(
            owner_client_id=self.owner_client_id,
            pool_id=self.pool_id,
            pool_token=self.pool_token,
            seq=seq,
            timeout_sec=_heartbeat_rpc_timeout_sec(self.heartbeat_timeout_sec),
        )
        now = _utc_now()
        self.last_heartbeat_at = now
        self.lease_expire_at = now + timedelta(seconds=max(1, int(self.heartbeat_timeout_sec or 0)))
        self.failed = False
        self.last_error = ""
        return resp

    def cancel_job(self, *, job_id: str, reason: str = "") -> pb2.CancelJobResponse:
        return self._client.cancel_pool_job(
            pool_id=self.pool_id,
            pool_token=self.pool_token,
            job_id=job_id,
            reason=reason,
        )

    def get_status(self) -> pb2.TaskPoolStatusInfo:
        info = self._client.get_task_pool_status(pool_id=self.pool_id, pool_token=self.pool_token)
        self.owner_client_id = str(info.owner_client_id or self.owner_client_id or "")
        self.pool_name = str(info.pool_name or self.pool_name or "")
        self.worker_count = max(0, int(info.worker_count or self.worker_count or 0))
        self.heartbeat_timeout_sec = max(1, int(info.heartbeat_timeout_sec or self.heartbeat_timeout_sec or 1))
        self.status = str(info.status or self.status or "")
        self.created_at = _timestamp_to_datetime(info.created_at)
        self.last_heartbeat_at = _timestamp_to_datetime(info.last_heartbeat_at)
        self.lease_expire_at = _timestamp_to_datetime(info.lease_expire_at)
        return info

    def update_globals_prepared(
        self,
        prepared_values: Dict[str, object],
        *,
        serialization_mode: str = "",
        effective_policy: Optional[EffectivePolicy] = None,
    ) -> pb2.UpdateRuntimeGlobalsResponse:
        kwargs = {}
        if str(serialization_mode or "").strip():
            kwargs["serialization_mode"] = serialization_mode
        if effective_policy is not None:
            kwargs["effective_policy"] = effective_policy
        return self._client.update_runtime_globals_prepared(
            client_id=self.pool_id,
            code_version=self.code_version,
            runtime_key=self.pool_id,
            code_token=self.pool_token,
            prepared_values=prepared_values,
            **kwargs,
        )

@dataclass
class ServiceSessionClient:
    """Low-level handle for one deployed service-session replica."""

    kind: str = field(init=False, default="service")
    _client: Any = field(repr=False)
    owner_client_id: str
    service_id: str
    service_token: str
    http_base_url: str
    heartbeat_timeout_sec: int
    worker_count: int
    status: int
    code_version: str = ""
    service_name: str = ""
    idle_ttl_sec: int = 0
    node_instance_id: str = ""
    node_id: str = ""
    created_at: Optional[datetime] = None
    last_heartbeat_at: Optional[datetime] = None
    lease_expire_at: Optional[datetime] = None
    failed: bool = False
    last_error: str = ""
    heartbeat_failure_threshold: int = 3

    @property
    def session_id(self) -> str:
        return str(self.service_id or "")

    @property
    def session_name(self) -> str:
        return str(self.service_name or self.service_id or "")

    @property
    def session_token(self) -> str:
        return str(self.service_token or "")

    def identity(self) -> SessionIdentity:
        return SessionIdentity(
            kind="service",
            session_id=self.session_id,
            session_name=self.session_name,
            owner_client_id=str(self.owner_client_id or ""),
            session_token=self.session_token,
        )

    def lease(self) -> SessionLease:
        created_at = self.created_at or self.last_heartbeat_at or _utc_now()
        last_heartbeat_at = self.last_heartbeat_at or created_at
        lease_expire_at = self.lease_expire_at or (last_heartbeat_at + timedelta(seconds=max(1, int(self.heartbeat_timeout_sec or 0))))
        return SessionLease(
            heartbeat_timeout_sec=max(1, int(self.heartbeat_timeout_sec or 0)),
            idle_ttl_sec=max(0, int(self.idle_ttl_sec or 0)),
            created_at=created_at,
            last_heartbeat_at=last_heartbeat_at,
            lease_expire_at=lease_expire_at,
        )

    def binding(self) -> SessionBinding:
        return SessionBinding(
            code_version=str(self.code_version or ""),
            worker_count=max(0, int(self.worker_count or 0)),
        )

    def snapshot(
        self,
        *,
        node_instance_id: str = "",
        node_id: str = "",
        failure: str = "",
    ) -> ExecutionReplicaSnapshot:
        lease = self.lease()
        try:
            status_text = pb2.ServiceStatus.Name(int(self.status or pb2.SERVICE_STATUS_UNSPECIFIED))
        except Exception:
            status_text = str(self.status or "")
        alive = not bool(self.failed) and not str(failure or "").strip() and int(self.status or 0) in {
            int(pb2.SERVICE_STATUS_STARTING),
            int(pb2.SERVICE_STATUS_RUNNING),
            int(pb2.SERVICE_STATUS_DRAINING),
        }
        return ExecutionReplicaSnapshot(
            kind="service",
            node_instance_id=str(node_instance_id or self.node_instance_id or ""),
            node_id=str(node_id or self.node_id or ""),
            session_id=self.session_id,
            session_name=self.session_name,
            code_version=str(self.code_version or ""),
            worker_count=max(0, int(self.worker_count or 0)),
            alive=alive,
            status=status_text,
            lease_expire_at=lease.lease_expire_at,
            failure=str(failure or self.last_error or ""),
        )

    def heartbeat(self) -> pb2.HeartbeatServiceResponse:
        resp = self._client.heartbeat_service(
            owner_client_id=self.owner_client_id,
            service_id=self.service_id,
            service_token=self.service_token,
            seq=0,
            timeout_sec=_heartbeat_rpc_timeout_sec(self.heartbeat_timeout_sec),
        )
        self.status = resp.status
        self.failed = False
        self.last_error = ""
        now = _utc_now()
        self.last_heartbeat_at = now
        self.lease_expire_at = now + timedelta(seconds=max(1, int(self.heartbeat_timeout_sec or 0)))
        return resp

    def end(self, reason: str = "client requested end") -> pb2.EndServiceResponse:
        resp = self._client.end_service(
            owner_client_id=self.owner_client_id,
            service_id=self.service_id,
            service_token=self.service_token,
            reason=reason,
        )
        self.status = resp.status
        return resp

    def close(self, reason: str = "client requested end") -> pb2.EndServiceResponse:
        return self.end(reason=reason)

    def get_status(self) -> pb2.ServiceStatusInfo:
        info = self._client.get_service_status(service_id=self.service_id)
        self.owner_client_id = str(info.owner_client_id or self.owner_client_id or "")
        self.service_name = str(info.service_name or self.service_name or "")
        self.worker_count = max(0, int(info.worker_count or self.worker_count or 0))
        self.created_at = _timestamp_to_datetime(info.created_at)
        self.last_heartbeat_at = _timestamp_to_datetime(info.last_heartbeat_at)
        self.lease_expire_at = _timestamp_to_datetime(info.lease_expire_at)
        self.status = info.status
        return info

    def list_methods(self, *, include_docs: bool = False) -> Sequence[pb2.ServiceMethodInfo]:
        return self._client.list_service_methods(service_id=self.service_id, include_docs=include_docs)

    def call(
        self,
        method: str,
        payload: Dict[str, object],
        *,
        timeout_sec: float = 60.0,
        token: Optional[str] = None,
        serialization_mode: str = "",
        effective_policy: Optional[EffectivePolicy] = None,
    ) -> Dict[str, object]:
        if not self.http_base_url:
            raise RuntimeError("service has no http_base_url; expose_http may be false")
        if not method:
            raise ValueError("method is required")

        auth_token = self.service_token if token is None else token
        prepare_kwargs = {}
        if str(serialization_mode or "").strip() and str(serialization_mode).strip().lower() != "legacy_v1":
            prepare_kwargs["serialization_mode"] = serialization_mode
        prepared_payload = prepare_remote_call_payload(
            [self._client],
            payload,
            effective_policy=effective_policy,
            **prepare_kwargs,
        )
        effective_timeout_sec = max(0.1, float(timeout_sec))
        encoded_payload = _serialize_http_call_payload(
            prepared_payload,
            context="service_call",
            mode=serialization_mode,
            effective_policy=effective_policy,
        )
        headers = {}
        if auth_token:
            headers["X-Service-Token"] = str(auth_token)
        try:
            body = runtime_http_request(
                base_url=str(self.http_base_url or "").strip(),
                control_addr=str(self._client.target or self.http_base_url or "").strip(),
                request=RuntimeTransportRequest(
                    path=f"/call/{quote(method, safe='')}?{urlencode({'timeout_sec': f'{effective_timeout_sec:.3f}'})}",
                    mode="json",
                    payload=encoded_payload,
                    timeout_sec=effective_timeout_sec,
                    method="POST",
                    headers=headers,
                ),
            )
        except Exception as exc:
            raise RuntimeError(f"call failed: {exc}") from exc
        if not body.get("ok", False):
            raise RuntimeError(f"call failed: {body.get('error', 'unknown error')}")
        return body

    def download_result_to_file(self, response_or_data: object, *, target_path: str) -> Path:
        ref = _extract_result_ref(response_or_data)
        if ref is None:
            raise ValueError("service result is inline data; no download needed")
        return self._client.download_result_to_file(ref, target_path=target_path)

    def fetch_result_data(self, response_or_data: object, *, target_path: str = ""):
        ref = _extract_result_ref(response_or_data)
        if ref is None:
            if isinstance(response_or_data, dict) and "data" in response_or_data:
                return response_or_data["data"]
            return response_or_data
        return self._client.fetch_result_ref_data(ref, target_path=target_path)

    def update_globals(self, values: Dict[str, object]) -> pb2.UpdateServiceGlobalsResponse:
        return self.update_globals_prepared(values)

    def update_globals_prepared(
        self,
        prepared_values: Dict[str, object],
        *,
        serialization_mode: str = "",
        effective_policy: Optional[EffectivePolicy] = None,
    ) -> pb2.UpdateServiceGlobalsResponse:
        kwargs = {}
        if str(serialization_mode or "").strip():
            kwargs["serialization_mode"] = serialization_mode
        if effective_policy is not None:
            kwargs["effective_policy"] = effective_policy
        return self._client.update_service_globals_prepared(
            owner_client_id=self.owner_client_id,
            service_id=self.service_id,
            service_token=self.service_token,
            prepared_values=prepared_values,
            **kwargs,
        )

    def update_globals_encoded(
        self,
        *,
        prepared_keys: Sequence[str],
        values: Optional[object] = None,
        transport_values: Optional[pb2.TransportPayload] = None,
    ) -> pb2.UpdateServiceGlobalsResponse:
        return self._client.update_service_globals_encoded(
            owner_client_id=self.owner_client_id,
            service_id=self.service_id,
            service_token=self.service_token,
            prepared_keys=prepared_keys,
            values=values,
            transport_values=transport_values,
        )
