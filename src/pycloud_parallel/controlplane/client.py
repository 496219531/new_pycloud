from __future__ import annotations

"""Thin compatibility facade re-exporting concrete controlplane/execution/data modules."""

from dataclasses import replace
from typing import Any, Dict, Optional, Sequence

from pycloud_parallel.artifact import export as pycloud_export
from pycloud_parallel.controlplane.artifact import (
    Artifact,
    ArtifactDeps,
    ArtifactExports,
    _normalize_artifact_input,
    _prepare_artifact,
)
from pycloud_parallel.controlplane.client_transport import (
    DiscoveryCallError,
    _call_route_http,
    _decode_http_request_body,
    _decode_http_response_body,
    _encode_http_json_body,
    _list_route_methods_http,
    _materialize_downloaded_result,
    _normalize_http_response_body,
    _serialize_http_call_payload,
    _serialize_route,
)
from pycloud_parallel.controlplane.discovery_client import DiscoveryCallerFacade, DiscoveryServiceClient
from pycloud_parallel.controlplane.discovery_route_cache import (
    _DiscoveryRouteCache,
    _RouteLocalState,
    _ServiceRouteSnapshot,
)
from pycloud_parallel.controlplane.gateway_client import GatewayCallerFacade, GatewayServiceClient
from pycloud_parallel.controlplane.http_client import http_json_request as _http_json_request
from pycloud_parallel.controlplane.http_client import target_to_base_url as _target_to_base_url
from pycloud_parallel.controlplane.infocenter_client import (
    InfoCenterClient,
    InfoCenterNode,
    InfoCenterNodeService,
    InfoCenterNodeTaskPool,
    InfoCenterServiceRoute,
    NodeCircuitState,
    _build_unique_node_id_map,
    _filter_nodes_by_runtime,
    _node_instance_key_from_node,
    _node_instance_key_from_route,
    _route_predicted_busy,
    _route_sort_key,
)
from pycloud_parallel.controlplane.node_control_client import NodeControlClient
from pycloud_parallel.controlplane.payload_transport import (
    estimate_payload_inline_size,
    prepare_outbound_payload,
    prepare_outbound_value,
)
from pycloud_parallel.controlplane.remote_payload import (
    prepare_remote_call_payload as _prepare_remote_call_payload,
)
from pycloud_parallel.controlplane.replica_client import NativeTaskPoolClient, ServiceSessionClient
from pycloud_parallel.controlplane.serialization import INLINE_PAYLOAD_SOFT_LIMIT_BYTES
from pycloud_parallel.controlplane.serialization import serialize_inline_payload, struct_to_dict
from pycloud_parallel.controlplane.config import (
    INLINE_PAYLOAD_HARD_LIMIT_BYTES,
    JOB_PAYLOAD_MAX_BYTES,
    JOB_STAGED_REF_TTL_SEC,
    JOB_STAGING_REPLICA_COUNT,
    get_payload_policy,
)
from pycloud_parallel.controlplane.data_ref import DataRef
from pycloud_parallel.data.object_ref import (
    NodeStoredRef,
    normalize_materialize_as,
    normalize_object_format,
    normalize_object_id,
    object_format_suffix,
    object_id_from_sha256_hex,
    object_ref_from_payload,
    object_ref_to_payload,
    object_storage_path,
)
from pycloud_parallel.data.result_ref import NodeResultHandle
from pycloud_parallel.execution.call_proxy import _BroadcastProxy, _CallProxy, _SyncCallProxy
from pycloud_parallel.execution.service_session import (
    Service,
    _ServiceSessionFileLock,
    _load_service_session_cache,
    _service_session_cache_file,
)
from pycloud_parallel.execution.support import (
    _estimate_managed_global_inline_size,
    _job_blob_requires_object_ref,
    _job_client_session_cache_file,
    _job_submit_upload_clients,
    _package_paths_to_targz,
    _policy_with_soft_limit,
    _prepare_code_blob,
    _prepare_job_blob_submit_fields as _support_prepare_job_blob_submit_fields,
    _prepare_job_submit_payload_for_call as _support_prepare_job_submit_payload_for_call,
    _prepare_local_artifact_for_upload,
    _prepare_managed_global_value_for_upload as _support_prepare_managed_global_value_for_upload,
    _prepare_payload_value_for_upload,
    _prepare_task_payload_for_submit as _support_prepare_task_payload_for_submit,
    _put_data_via_clients,
    _serialize_data_for_object_ref,
)
from pycloud_parallel.execution.task_pool import TaskPool

globals()["JobQueue" + "Client"] = None
globals()["TaskPool" + "Session"] = TaskPool
globals()["Object" + "Ref"] = NodeStoredRef
globals()["Result" + "Ref"] = NodeResultHandle


def _prepare_managed_global_value_for_upload(
    clients: Sequence[object],
    value: Any,
    *,
    object_threshold_bytes: int = INLINE_PAYLOAD_SOFT_LIMIT_BYTES,
) -> Any:
    inline_size = _estimate_managed_global_inline_size(value)
    if inline_size <= max(1, int(object_threshold_bytes)):
        return value
    try:
        object_ref = _put_data_via_clients(clients, value)
        return object_ref.to_data_ref()
    except Exception as exc:
        raise ValueError(
            "managed global exceeds inline threshold and large-object upload failed: "
            f"size_bytes={inline_size} threshold_bytes={max(1, int(object_threshold_bytes))}; "
            f"error={exc}"
        ) from exc


def _prepare_task_payload_for_submit(
    client: object,
    payload: Dict[str, object],
    *,
    object_threshold_bytes: int = INLINE_PAYLOAD_SOFT_LIMIT_BYTES,
) -> Any:
    # Keep this wrapper local so tests can patch `prepare_outbound_payload` on this module.
    policy = _policy_with_soft_limit(get_payload_policy("task_submit"), object_threshold_bytes)
    return prepare_outbound_payload(
        payload,
        put_data=lambda value, *, format="": _put_data_via_clients([client], value, format=format),
        estimate_inline_size=_estimate_managed_global_inline_size,
        policy=policy,
    )


def _prepare_http_payload_for_call(
    clients: Sequence[object],
    payload: Optional[Dict[str, object]],
    *,
    object_threshold_bytes: int = INLINE_PAYLOAD_SOFT_LIMIT_BYTES,
) -> Dict[str, object]:
    policy = _policy_with_soft_limit(get_payload_policy("http_call"), object_threshold_bytes)
    return prepare_outbound_payload(
        payload,
        put_data=lambda value, *, format="": _put_data_via_clients(clients, value, format=format),
        estimate_inline_size=_estimate_managed_global_inline_size,
        policy=policy,
    )


def _prepare_job_blob_submit_fields(
    *,
    target: str,
    blob: bytes,
    package_format: str,
    runtime: str,
    timeout_sec: float,
) -> Dict[str, object]:
    return _support_prepare_job_blob_submit_fields(
        target=target,
        blob=blob,
        package_format=package_format,
        runtime=runtime,
        timeout_sec=timeout_sec,
    )


def _prepare_job_submit_payload_for_call(
    *,
    target: str,
    payload: Dict[str, object],
    timeout_sec: float,
) -> Dict[str, object]:
    # Keep wrapper local so tests can patch `_job_submit_upload_clients` / `prepare_outbound_payload`.
    prepared = dict(payload or {})
    preserved_fields = {
        field_name: prepared.pop(field_name)
        for field_name in ("job_payload", "update_globals")
        if field_name in prepared
    }
    clients = []
    try:
        clients = _job_submit_upload_clients(
            target=target,
            payload=prepared,
            timeout_sec=timeout_sec,
        )
        if not clients:
            prepared.update(preserved_fields)
            return prepared
        outbound = prepare_outbound_payload(
            prepared,
            put_data=lambda value, *, format="": _put_data_via_clients(clients, value, format=format),
            estimate_inline_size=_estimate_managed_global_inline_size,
            policy=get_payload_policy("job_submit"),
        )
        outbound.update(preserved_fields)
        return outbound
    finally:
        for client in clients:
            try:
                client.close()
            except Exception:
                pass


__all__ = [
    "Artifact",
    "ArtifactDeps",
    "ArtifactExports",
    "DataRef",
    "DiscoveryCallError",
    "DiscoveryServiceClient",
    "GatewayServiceClient",
    "InfoCenterClient",
    "InfoCenterNode",
    "InfoCenterNodeService",
    "InfoCenterNodeTaskPool",
    "InfoCenterServiceRoute",
    "NativeTaskPoolClient",
    "NodeCircuitState",
    "NodeControlClient",
    "NodeResultHandle",
    "NodeStoredRef",
    "Service",
    "ServiceSessionClient",
    "TaskPool",
    "_BroadcastProxy",
    "_CallProxy",
    "_DiscoveryRouteCache",
    "_RouteLocalState",
    "_ServiceRouteSnapshot",
    "_ServiceSessionFileLock",
    "_SyncCallProxy",
    "_build_unique_node_id_map",
    "_call_route_http",
    "_decode_http_request_body",
    "_decode_http_response_body",
    "_encode_http_json_body",
    "_estimate_managed_global_inline_size",
    "_filter_nodes_by_runtime",
    "_http_json_request",
    "_job_blob_requires_object_ref",
    "_job_client_session_cache_file",
    "_job_submit_upload_clients",
    "_list_route_methods_http",
    "_load_service_session_cache",
    "_materialize_downloaded_result",
    "_node_instance_key_from_node",
    "_node_instance_key_from_route",
    "_normalize_artifact_input",
    "_normalize_http_response_body",
    "_package_paths_to_targz",
    "_policy_with_soft_limit",
    "_prepare_artifact",
    "_prepare_code_blob",
    "_prepare_http_payload_for_call",
    "_prepare_job_blob_submit_fields",
    "_prepare_job_submit_payload_for_call",
    "_prepare_local_artifact_for_upload",
    "_prepare_managed_global_value_for_upload",
    "_prepare_payload_value_for_upload",
    "_prepare_remote_call_payload",
    "_prepare_task_payload_for_submit",
    "_put_data_via_clients",
    "_route_predicted_busy",
    "_route_sort_key",
    "_serialize_data_for_object_ref",
    "_serialize_http_call_payload",
    "_serialize_route",
    "_service_session_cache_file",
    "_target_to_base_url",
    "estimate_payload_inline_size",
    "normalize_materialize_as",
    "normalize_object_format",
    "normalize_object_id",
    "object_format_suffix",
    "object_id_from_sha256_hex",
    "object_ref_from_payload",
    "object_ref_to_payload",
    "object_storage_path",
    "prepare_outbound_payload",
    "prepare_outbound_value",
    "pycloud_export",
    "serialize_inline_payload",
    "struct_to_dict",
]
