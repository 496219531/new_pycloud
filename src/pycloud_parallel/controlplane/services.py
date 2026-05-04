from __future__ import annotations

"""Legacy NodeControl method compatibility helpers.

The runtime now uses HTTP NodeControl transport. This module keeps a small
in-process method surface so tests and internal callers can exercise the same
business logic without any legacy server dependency.
"""

import logging
from typing import Optional

from pycloud_parallel.controlplane.payload_transport import decode_payload_from_transport
from pycloud_parallel.controlplane.serialization import (
    decode_transport_payload_bytes,
    detect_transport_mode,
    dict_to_struct,
    make_validated_inline_transport_carrier,
    struct_to_python,
    value_to_transport_payload,
    validate_inline_payload_structs,
    validate_inline_request_size,
    validate_transport_payload_bytes,
)
from pycloud_parallel.controlplane.config import get_payload_policy
from pycloud_parallel.proto.v1 import pycloud_v1_pb2 as pb2

logger = logging.getLogger(__name__)


def _err(code: int, message: str, request_id: str = "") -> pb2.Error:
    return pb2.Error(code=code, message=message, request_id=request_id)


def _set_context_error(context, code_name: str, details: str) -> None:
    if context is None:
        return
    try:
        context.set_code(code_name)
    except Exception:
        pass
    try:
        context.set_details(details)
    except Exception:
        pass


class NodeControlService:
    def __init__(self, state, *, on_service_routes_changed=None) -> None:
        self._state = state
        self._on_service_routes_changed = on_service_routes_changed

    def _notify_service_routes_changed(self) -> None:
        if self._on_service_routes_changed is None:
            return
        try:
            self._on_service_routes_changed()
        except Exception:
            logger.exception("[NodeControl] service route sync callback failed")

    def UpdateRuntimeGlobals(self, request: pb2.UpdateRuntimeGlobalsRequest, context) -> pb2.UpdateRuntimeGlobalsResponse:
        if not request.client_id or not request.code_version or not request.code_token:
            _set_context_error(context, "INVALID_ARGUMENT", "client_id, code_version and code_token are required")
            return pb2.UpdateRuntimeGlobalsResponse(
                ok=False,
                code_version=request.code_version,
                runtime_key=request.runtime_key or request.code_version,
                error=_err(pb2.ERROR_CODE_INVALID_REQUEST, "client_id, code_version and code_token are required"),
            )
        try:
            self._state.require_runtime_globals_update_authorized(
                client_id=request.client_id,
                code_version=request.code_version,
                code_token=request.code_token,
            )
            if request.HasField("transport_values") and str(request.transport_values.codec or "").strip():
                serialization_mode = str(request.transport_values.codec or "").strip().lower()
                decoded_values = decode_transport_payload_bytes(
                    request.transport_values.codec,
                    request.transport_values.version,
                    request.transport_values.payload,
                    context="taskpool_session",
                )
            else:
                raw_values = struct_to_python(request.values)
                serialization_mode = detect_transport_mode(raw_values, default="legacy_v1")
                decoded_values = decode_payload_from_transport(
                    raw_values,
                    policy=get_payload_policy("managed_globals"),
                    mode=serialization_mode,
                    context="taskpool_session",
                )
            globals_digest, updated_names = self._state.update_runtime_globals(
                client_id=request.client_id,
                code_version=request.code_version,
                runtime_key=request.runtime_key,
                code_token=request.code_token,
                values=decoded_values,
                serialization_mode=serialization_mode,
            )
        except KeyError as exc:
            _set_context_error(context, "NOT_FOUND", str(exc))
            return pb2.UpdateRuntimeGlobalsResponse(
                ok=False,
                code_version=request.code_version,
                runtime_key=request.runtime_key or request.code_version,
                error=_err(pb2.ERROR_CODE_TASK_NOT_FOUND, str(exc)),
            )
        except PermissionError as exc:
            _set_context_error(context, "PERMISSION_DENIED", str(exc))
            return pb2.UpdateRuntimeGlobalsResponse(
                ok=False,
                code_version=request.code_version,
                runtime_key=request.runtime_key or request.code_version,
                error=_err(pb2.ERROR_CODE_UNAUTHORIZED, str(exc)),
            )
        except ValueError as exc:
            _set_context_error(context, "INVALID_ARGUMENT", str(exc))
            return pb2.UpdateRuntimeGlobalsResponse(
                ok=False,
                code_version=request.code_version,
                runtime_key=request.runtime_key or request.code_version,
                error=_err(pb2.ERROR_CODE_INVALID_REQUEST, str(exc)),
            )
        return pb2.UpdateRuntimeGlobalsResponse(
            ok=True,
            code_version=request.code_version,
            runtime_key=request.runtime_key or request.code_version,
            globals_digest=globals_digest,
            updated_names=updated_names,
        )

    def CallService(self, request: pb2.CallServiceRequest, context) -> pb2.CallServiceResponse:
        if not request.service_id or not request.method:
            _set_context_error(context, "INVALID_ARGUMENT", "service_id and method are required")
            return pb2.CallServiceResponse(
                ok=False,
                service_id=request.service_id,
                method=request.method,
                error=_err(pb2.ERROR_CODE_INVALID_REQUEST, "service_id and method are required"),
            )
        try:
            if not request.HasField("transport_payload") or not str(request.transport_payload.codec or "").strip():
                validate_inline_payload_structs(
                    [request.payload],
                    item_context="service call payload",
                    request_context="call service request",
                )
        except ValueError as exc:
            _set_context_error(context, "INVALID_ARGUMENT", str(exc))
            return pb2.CallServiceResponse(
                ok=False,
                service_id=request.service_id,
                method=request.method,
                error=_err(pb2.ERROR_CODE_INVALID_REQUEST, str(exc)),
            )

        request_uses_transport_payload = bool(
            request.HasField("transport_payload") and str(request.transport_payload.codec or "").strip()
        )
        if request_uses_transport_payload:
            payload_policy = get_payload_policy("http_call")
            request_serialization_mode, raw_payload, request_payload_size = validate_transport_payload_bytes(
                request.transport_payload.codec,
                request.transport_payload.version,
                request.transport_payload.payload,
                context="service_owner",
                limit_bytes=payload_policy.inline_payload_hard_limit_bytes,
            )
            validate_inline_request_size(
                request_payload_size,
                limit_bytes=payload_policy.inline_payload_request_limit_bytes,
                context="call service request",
            )
            decoded_payload = make_validated_inline_transport_carrier(
                codec=request_serialization_mode,
                payload=raw_payload,
                content_size=request_payload_size,
                payload_mode="service_call",
                context="service_owner",
            )
        else:
            raw_payload = struct_to_python(request.payload)
            request_serialization_mode = detect_transport_mode(raw_payload, default="legacy_v1")
            decoded_payload = decode_payload_from_transport(
                raw_payload,
                policy=get_payload_policy("http_call"),
                mode=request_serialization_mode,
                context="service_owner",
            )

        code, body = self._state.call_service(
            service_id=request.service_id,
            method=request.method,
            payload=decoded_payload,
            service_token=request.service_token,
            timeout_sec=max(0.1, float(request.timeout_sec or 60.0)),
            serialization_mode=request_serialization_mode,
            use_transport_result=request_uses_transport_payload,
        )
        if code == 404:
            _set_context_error(context, "NOT_FOUND", str(body.get("error", "service/method not found")))
            return pb2.CallServiceResponse(
                ok=False,
                service_id=request.service_id,
                method=request.method,
                error=_err(pb2.ERROR_CODE_TASK_NOT_FOUND, str(body.get("error", "service/method not found"))),
            )
        if code == 401:
            _set_context_error(context, "PERMISSION_DENIED", str(body.get("error", "unauthorized")))
            return pb2.CallServiceResponse(
                ok=False,
                service_id=request.service_id,
                method=request.method,
                error=_err(pb2.ERROR_CODE_UNAUTHORIZED, str(body.get("error", "unauthorized"))),
            )
        if code >= 500:
            _set_context_error(context, "INTERNAL", str(body.get("error", "call service failed")))
            return pb2.CallServiceResponse(
                ok=False,
                service_id=request.service_id,
                method=request.method,
                error=_err(pb2.ERROR_CODE_INTERNAL_ERROR, str(body.get("error", "call service failed"))),
            )
        if not body.get("ok", False):
            _set_context_error(context, "FAILED_PRECONDITION", str(body.get("error", "call rejected")))
            return pb2.CallServiceResponse(
                ok=False,
                service_id=request.service_id,
                method=request.method,
                task_error=pb2.TaskError(
                    type=str(body.get("error_type", "UserError")),
                    message=str(body.get("error", "call rejected")),
                ),
                error=_err(pb2.ERROR_CODE_INVALID_REQUEST, str(body.get("error", "call rejected"))),
            )

        response = pb2.CallServiceResponse(
            ok=True,
            service_id=request.service_id,
            method=request.method,
        )
        if request_uses_transport_payload and body.get("transport_data"):
            response.transport_data.CopyFrom(body["transport_data"])
        elif request_uses_transport_payload:
            response.transport_data.CopyFrom(
                value_to_transport_payload(
                    body.get("data", {}),
                    mode=request_serialization_mode,
                    context="service_result",
                    limit_bytes=get_payload_policy("result").inline_result_hard_limit_bytes,
                    reject_transport_envelope=True,
                )
            )
        else:
            response.data.CopyFrom(dict_to_struct(body.get("data", {}), mode=request_serialization_mode))
        return response

    def UpdateServiceGlobals(self, request: pb2.UpdateServiceGlobalsRequest, context) -> pb2.UpdateServiceGlobalsResponse:
        if not request.owner_client_id or not request.service_id or not request.service_token:
            _set_context_error(context, "INVALID_ARGUMENT", "owner_client_id, service_id and service_token are required")
            return pb2.UpdateServiceGlobalsResponse(
                ok=False,
                service_id=request.service_id,
                error=_err(pb2.ERROR_CODE_INVALID_REQUEST, "owner_client_id, service_id and service_token are required"),
            )
        try:
            self._state.require_service_globals_update_authorized(
                owner_client_id=request.owner_client_id,
                service_id=request.service_id,
                service_token=request.service_token,
            )
            if request.HasField("transport_values") and str(request.transport_values.codec or "").strip():
                serialization_mode = str(request.transport_values.codec or "").strip().lower()
                decoded_values = decode_transport_payload_bytes(
                    request.transport_values.codec,
                    request.transport_values.version,
                    request.transport_values.payload,
                    context="service_owner",
                )
            else:
                raw_values = struct_to_python(request.values)
                serialization_mode = detect_transport_mode(raw_values, default="legacy_v1")
                decoded_values = decode_payload_from_transport(
                    raw_values,
                    policy=get_payload_policy("managed_globals"),
                    mode=serialization_mode,
                    context="service_owner",
                )
            globals_digest, updated_names = self._state.update_service_globals(
                owner_client_id=request.owner_client_id,
                service_id=request.service_id,
                service_token=request.service_token,
                values=decoded_values,
                serialization_mode=serialization_mode,
            )
        except KeyError as exc:
            _set_context_error(context, "NOT_FOUND", str(exc))
            return pb2.UpdateServiceGlobalsResponse(
                ok=False,
                service_id=request.service_id,
                error=_err(pb2.ERROR_CODE_TASK_NOT_FOUND, str(exc)),
            )
        except PermissionError as exc:
            _set_context_error(context, "PERMISSION_DENIED", str(exc))
            return pb2.UpdateServiceGlobalsResponse(
                ok=False,
                service_id=request.service_id,
                error=_err(pb2.ERROR_CODE_UNAUTHORIZED, str(exc)),
            )
        except ValueError as exc:
            _set_context_error(context, "INVALID_ARGUMENT", str(exc))
            return pb2.UpdateServiceGlobalsResponse(
                ok=False,
                service_id=request.service_id,
                error=_err(pb2.ERROR_CODE_INVALID_REQUEST, str(exc)),
            )
        return pb2.UpdateServiceGlobalsResponse(
            ok=True,
            service_id=request.service_id,
            globals_digest=globals_digest,
            updated_names=updated_names,
        )

    def CloseTaskPool(self, request: pb2.CloseTaskPoolRequest, context) -> pb2.CloseTaskPoolResponse:
        try:
            result = self._state.close_task_pool(
                owner_client_id=request.owner_client_id,
                pool_id=request.pool_id,
                pool_token=request.pool_token,
                reason=request.reason,
            )
        except KeyError as exc:
            _set_context_error(context, "NOT_FOUND", str(exc))
            return pb2.CloseTaskPoolResponse(
                ok=False,
                error=_err(pb2.ERROR_CODE_TASK_NOT_FOUND, str(exc)),
            )
        except PermissionError as exc:
            _set_context_error(context, "PERMISSION_DENIED", str(exc))
            return pb2.CloseTaskPoolResponse(
                ok=False,
                error=_err(pb2.ERROR_CODE_UNAUTHORIZED, str(exc)),
            )
        accepted = bool(getattr(result, "accepted", True))
        self._notify_service_routes_changed()
        return pb2.CloseTaskPoolResponse(ok=True, accepted=accepted)


__all__ = ["NodeControlService"]
