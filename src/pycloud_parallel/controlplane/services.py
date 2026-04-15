from __future__ import annotations

"""gRPC service implementations for PyCloud NodeControl task/service APIs."""

import logging
import hashlib
import os
import tempfile
from pathlib import Path
from typing import Callable, Iterable, List, Optional

import grpc

from pycloud_parallel.controlplane.config import OBJECT_CHUNK_SIZE_BYTES, get_payload_policy
from pycloud_parallel.controlplane.payload_transport import decode_payload_from_transport
from pycloud_parallel.controlplane.state import NodeControlState, dt_to_ts, struct_to_dict, touch_object_last_at
from pycloud_parallel.controlplane.serialization import dict_to_struct, log_payload_flow, validate_inline_payload_structs
from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2
from pycloud_parallel.grpc.v1 import pycloud_v1_pb2_grpc as pb2_grpc

logger = logging.getLogger(__name__)


def _err(code: int, message: str, request_id: str = "") -> pb2.Error:
    return pb2.Error(code=code, message=message, request_id=request_id)


def _tempfile_suffix_for_package_format(package_format: str) -> str:
    normalized = str(package_format or "").strip().lower()
    if normalized == "tar.gz":
        return ".tar.gz"
    if normalized == "zip":
        return ".zip"
    if normalized == "whl":
        return ".whl"
    if normalized == "py":
        return ".py"
    return ".bin"


def _service_info_to_pb(info: dict) -> pb2.ServiceStatusInfo:
    return pb2.ServiceStatusInfo(
        service_id=str(info.get("service_id", "")),
        owner_client_id=str(info.get("owner_client_id", "")),
        service_name=str(info.get("service_name", "")),
        code_version=str(info.get("code_version", "")),
        status=int(info.get("status", pb2.SERVICE_STATUS_UNSPECIFIED)),
        worker_count=int(info.get("worker_count", 0)),
        alive_workers=int(info.get("alive_workers", 0)),
        in_flight=int(info.get("in_flight", 0)),
        queued=int(info.get("queued", 0)),
        created_at=dt_to_ts(info["created_at"]),
        last_heartbeat_at=dt_to_ts(info["last_heartbeat_at"]),
        lease_expire_at=dt_to_ts(info["lease_expire_at"]),
        http_base_url=str(info.get("http_base_url", "")),
    )


def _service_route_to_pb(info: dict) -> pb2.ServiceRouteInfo:
    return pb2.ServiceRouteInfo(
        service_name=str(info.get("service_name", "")),
        service_id=str(info.get("service_id", "")),
        status=int(info.get("status", pb2.SERVICE_STATUS_UNSPECIFIED)),
        node_id=str(info.get("node_id", "")),
        control_addr=str(info.get("control_addr", "")),
        node_healthy=bool(info.get("node_healthy", False)),
        worker_count=int(info.get("worker_count", 0)),
        alive_workers=int(info.get("alive_workers", 0)),
        in_flight=int(info.get("in_flight", 0)),
        lease_expire_at=dt_to_ts(info["lease_expire_at"]),
        http_base_url=str(info.get("http_base_url", "")),
    )


def _peer(context: grpc.ServicerContext) -> str:
    try:
        return context.peer()
    except Exception:
        return "unknown-peer"


class NodeControlService(pb2_grpc.NodeControlServiceServicer):
    """NodeControl gRPC 服务。

    负责代码/对象上传、TaskPool 控制面和服务会话模式的 gRPC 能力。

    Attributes:
        _state: NodeControl 状态管理器
    """

    def __init__(self, state: NodeControlState, *, on_service_routes_changed: Optional[Callable[[], None]] = None) -> None:
        self._state = state
        self._on_service_routes_changed = on_service_routes_changed

    def _notify_service_routes_changed(self) -> None:
        if self._on_service_routes_changed is None:
            return
        try:
            self._on_service_routes_changed()
        except Exception:
            logger.exception("[NodeControl] service route sync callback failed")

    def UploadCode(self, request_iterator: Iterable[pb2.UploadCodeRequest], context: grpc.ServicerContext) -> pb2.UploadCodeResponse:
        meta = None
        chunk_count = 0
        size_bytes = 0
        h = hashlib.sha256()
        tmp_file = None
        tmp_path = ""
        for req in request_iterator:
            kind = req.WhichOneof("body")
            if kind == "meta":
                if meta is not None:
                    context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                    context.set_details("meta frame can only appear once")
                    return pb2.UploadCodeResponse(
                        ok=False,
                        error=_err(pb2.ERROR_CODE_INVALID_REQUEST, "meta frame can only appear once"),
                    )
                meta = req.meta
            elif kind == "chunk":
                if meta is None:
                    context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                    context.set_details("meta frame must come before chunk frames")
                    return pb2.UploadCodeResponse(
                        ok=False,
                        error=_err(pb2.ERROR_CODE_INVALID_REQUEST, "meta frame must come before chunk frames"),
                    )
                if tmp_file is None:
                    tmp_file = tempfile.NamedTemporaryFile(
                        mode="wb",
                        prefix="pycloud-upload-",
                        suffix=_tempfile_suffix_for_package_format(meta.package_format),
                        delete=False,
                        dir=str(self._state.artifact_dir),
                    )
                    tmp_path = tmp_file.name
                part = req.chunk or b""
                if part:
                    tmp_file.write(part)
                    h.update(part)
                    size_bytes += len(part)
                    chunk_count += 1

        logger.info(
            "[NodeControl] UploadCode peer=%s client_id=%s package_format=%s entry_module=%s chunks=%d",
            _peer(context),
            (meta.client_id if meta is not None else ""),
            (meta.package_format if meta is not None else ""),
            (meta.entry_module if meta is not None else ""),
            chunk_count,
        )

        if meta is None:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("missing upload metadata frame")
            logger.warning("[NodeControl] UploadCode missing meta peer=%s", _peer(context))
            return pb2.UploadCodeResponse(
                ok=False,
                error=_err(pb2.ERROR_CODE_INVALID_REQUEST, "missing upload metadata frame"),
            )

        if tmp_file is None:
            tmp_file = tempfile.NamedTemporaryFile(
                mode="wb",
                prefix="pycloud-upload-",
                suffix=".bin",
                delete=False,
                dir=str(self._state.artifact_dir),
            )
            tmp_path = tmp_file.name

        try:
            tmp_file.close()
            export_spec = meta.export_spec
            artifact, cached = self._state.put_code_from_uploaded_file(
                client_id=meta.client_id,
                sha256=meta.sha256,
                runtime=meta.runtime,
                entry_module=meta.entry_module,
                entry_callable=meta.entry_callable,
                package_format=meta.package_format,
                export_mode=export_spec.mode,
                export_methods=list(export_spec.methods),
                export_decorator=export_spec.decorator,
                dependency_policy_mode=meta.dependency_policy_mode,
                dependency_allowlist=list(meta.dependency_allowlist),
                managed_global_names=list(meta.managed_global_names),
                code_token=meta.code_token,
                uploaded_path=tmp_path,
                actual_sha256=h.hexdigest(),
                size_bytes=size_bytes,
                validate_load=True,
            )
            effective_code_token = self._state.get_client_code_token(
                client_id=meta.client_id,
                code_version=artifact.code_version,
            )
        except ValueError as exc:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(exc))
            logger.warning("[NodeControl] UploadCode invalid request peer=%s err=%s", _peer(context), str(exc))
            return pb2.UploadCodeResponse(
                ok=False,
                error=_err(pb2.ERROR_CODE_INVALID_REQUEST, str(exc)),
            )
        finally:
            if tmp_path:
                try:
                    os.unlink(tmp_path)
                except FileNotFoundError:
                    pass

        logger.info(
            "[NodeControl] UploadCode ok peer=%s code_version=%s cached=%s size_bytes=%d",
            _peer(context),
            artifact.code_version,
            bool(cached),
            int(artifact.size_bytes),
        )
        return pb2.UploadCodeResponse(
            ok=True,
            code_version=artifact.code_version,
            cached=cached,
            size_bytes=artifact.size_bytes,
            created_at=dt_to_ts(artifact.created_at),
            code_token=effective_code_token,
        )

    def UploadObject(self, request_iterator: Iterable[pb2.UploadObjectRequest], context: grpc.ServicerContext) -> pb2.UploadObjectResponse:
        meta = None
        chunk_count = 0
        size_bytes = 0
        h = hashlib.sha256()
        tmp_file = None
        tmp_path = ""

        def _ensure_object_dir() -> None:
            self._state.object_dir.mkdir(parents=True, exist_ok=True)

        for req in request_iterator:
            kind = req.WhichOneof("body")
            if kind == "meta":
                if meta is not None:
                    context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                    context.set_details("meta frame can only appear once")
                    return pb2.UploadObjectResponse(
                        ok=False,
                        error=_err(pb2.ERROR_CODE_INVALID_REQUEST, "meta frame can only appear once"),
                    )
                meta = req.meta
            elif kind == "chunk":
                if meta is None:
                    context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                    context.set_details("meta frame must come before chunk frames")
                    return pb2.UploadObjectResponse(
                        ok=False,
                        error=_err(pb2.ERROR_CODE_INVALID_REQUEST, "meta frame must come before chunk frames"),
                    )
                if tmp_file is None:
                    _ensure_object_dir()
                    tmp_file = tempfile.NamedTemporaryFile(
                        mode="wb",
                        prefix="pycloud-object-",
                        suffix=_tempfile_suffix_for_package_format(meta.format or "bin"),
                        delete=False,
                        dir=str(self._state.object_dir),
                    )
                    tmp_path = tmp_file.name
                part = req.chunk or b""
                if part:
                    tmp_file.write(part)
                    h.update(part)
                    size_bytes += len(part)
                    chunk_count += 1

        logger.info(
            "[NodeControl] UploadObject peer=%s object_id=%s format=%s chunks=%d",
            _peer(context),
            (meta.object_id if meta is not None else ""),
            (meta.format if meta is not None else ""),
            chunk_count,
        )

        if meta is None:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("missing object metadata frame")
            return pb2.UploadObjectResponse(
                ok=False,
                error=_err(pb2.ERROR_CODE_INVALID_REQUEST, "missing object metadata frame"),
            )

        if tmp_file is None:
            _ensure_object_dir()
            tmp_file = tempfile.NamedTemporaryFile(
                mode="wb",
                prefix="pycloud-object-",
                suffix=".bin",
                delete=False,
                dir=str(self._state.object_dir),
            )
            tmp_path = tmp_file.name

        try:
            tmp_file.close()
            artifact, cached = self._state.data_store.put_uploaded_file(
                object_id=meta.object_id,
                format=meta.format,
                uploaded_path=tmp_path,
                actual_sha256=h.hexdigest(),
                size_bytes=size_bytes,
            )
        except ValueError as exc:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(exc))
            return pb2.UploadObjectResponse(
                ok=False,
                error=_err(pb2.ERROR_CODE_INVALID_REQUEST, str(exc)),
            )
        finally:
            if tmp_path:
                try:
                    os.unlink(tmp_path)
                except FileNotFoundError:
                    pass

        return pb2.UploadObjectResponse(
            ok=True,
            object_id=artifact.object_id,
            format=artifact.format,
            cached=cached,
            size_bytes=artifact.size_bytes,
            created_at=dt_to_ts(artifact.created_at),
        )

    def DownloadObject(
        self,
        request: pb2.DownloadObjectRequest,
        context: grpc.ServicerContext,
    ) -> Iterable[pb2.DownloadObjectChunk]:
        object_id = str(request.object_id or "").strip()
        if not object_id:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("object_id is required")
            return
        try:
            artifact = self._state.get_object_artifact(object_id)
        except KeyError:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details("object not found")
            return

        try:
            if getattr(artifact, "storage_backend", "file") == "segment":
                touch_object_last_at(self._state.object_dir, object_id=artifact.object_id, fallback_path=Path(artifact.segment_path))
                with open(artifact.segment_path, "rb") as fp:
                    fp.seek(max(0, int(getattr(artifact, "segment_offset", 0) or 0)))
                    remaining = max(0, int(getattr(artifact, "segment_length", artifact.size_bytes) or artifact.size_bytes))
                    while remaining > 0:
                        part = fp.read(min(OBJECT_CHUNK_SIZE_BYTES, remaining))
                        if not part:
                            break
                        remaining -= len(part)
                        yield pb2.DownloadObjectChunk(
                            object_id=artifact.object_id,
                            format=artifact.format,
                            size_bytes=artifact.size_bytes,
                            chunk=part,
                        )
                return

            touch_object_last_at(self._state.object_dir, object_id=artifact.object_id, fallback_path=Path(artifact.path))
            with open(artifact.path, "rb") as fp:
                while True:
                    part = fp.read(OBJECT_CHUNK_SIZE_BYTES)
                    if not part:
                        break
                    yield pb2.DownloadObjectChunk(
                        object_id=artifact.object_id,
                        format=artifact.format,
                        size_bytes=artifact.size_bytes,
                        chunk=part,
                    )
        except FileNotFoundError:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details("object file missing")
            return

    def PinObject(
        self,
        request: pb2.PinObjectRequest,
        context: grpc.ServicerContext,
    ) -> pb2.PinObjectResponse:
        object_id = str(request.object_id or "").strip()
        ref_id = str(request.ref_id or "").strip()
        if not object_id or not ref_id:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("object_id and ref_id are required")
            return pb2.PinObjectResponse(
                ok=False,
                pinned=False,
                error=_err(pb2.ERROR_CODE_INVALID_REQUEST, "object_id and ref_id are required"),
            )
        try:
            pinned = self._state.pin_object(object_id, ref_id=ref_id)
        except ValueError as exc:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(exc))
            return pb2.PinObjectResponse(
                ok=False,
                pinned=False,
                error=_err(pb2.ERROR_CODE_INVALID_REQUEST, str(exc)),
            )
        if not pinned:
            return pb2.PinObjectResponse(
                ok=False,
                pinned=False,
                error=_err(pb2.ERROR_CODE_TASK_NOT_FOUND, "object not found"),
            )
        return pb2.PinObjectResponse(ok=True, pinned=True)

    def ReleaseObject(
        self,
        request: pb2.ReleaseObjectRequest,
        context: grpc.ServicerContext,
    ) -> pb2.ReleaseObjectResponse:
        object_id = str(request.object_id or "").strip()
        ref_id = str(request.ref_id or "").strip()
        if not object_id:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("object_id is required")
            return pb2.ReleaseObjectResponse(
                ok=False,
                released=False,
                error=_err(pb2.ERROR_CODE_INVALID_REQUEST, "object_id is required"),
            )
        try:
            released = self._state.release_object(object_id, ref_id=ref_id)
        except ValueError as exc:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(exc))
            return pb2.ReleaseObjectResponse(
                ok=False,
                released=False,
                error=_err(pb2.ERROR_CODE_INVALID_REQUEST, str(exc)),
            )
        if not released:
            return pb2.ReleaseObjectResponse(
                ok=False,
                released=False,
                error=_err(pb2.ERROR_CODE_TASK_NOT_FOUND, "object not found"),
            )
        return pb2.ReleaseObjectResponse(ok=True, released=True)

    def UpdateRuntimeGlobals(
        self,
        request: pb2.UpdateRuntimeGlobalsRequest,
        context: grpc.ServicerContext,
    ) -> pb2.UpdateRuntimeGlobalsResponse:
        logger.info(
            "[NodeControl] UpdateRuntimeGlobals peer=%s client_id=%s code_version=%s runtime_key=%s",
            _peer(context),
            request.client_id,
            request.code_version,
            request.runtime_key,
        )
        try:
            globals_digest, updated_names = self._state.update_runtime_globals(
                client_id=request.client_id,
                code_version=request.code_version,
                runtime_key=request.runtime_key,
                code_token=request.code_token,
                values=decode_payload_from_transport(
                    struct_to_dict(request.values),
                    policy=get_payload_policy("managed_globals"),
                ),
            )
            return pb2.UpdateRuntimeGlobalsResponse(
                ok=True,
                code_version=request.code_version,
                runtime_key=request.runtime_key or request.code_version,
                globals_digest=globals_digest,
                updated_names=updated_names,
            )
        except KeyError as exc:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(str(exc))
            return pb2.UpdateRuntimeGlobalsResponse(
                ok=False,
                code_version=request.code_version,
                runtime_key=request.runtime_key or request.code_version,
                error=_err(pb2.ERROR_CODE_TASK_NOT_FOUND, str(exc)),
            )
        except PermissionError as exc:
            context.set_code(grpc.StatusCode.PERMISSION_DENIED)
            context.set_details(str(exc))
            return pb2.UpdateRuntimeGlobalsResponse(
                ok=False,
                code_version=request.code_version,
                runtime_key=request.runtime_key or request.code_version,
                error=_err(pb2.ERROR_CODE_UNAUTHORIZED, str(exc)),
            )
        except ValueError as exc:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(exc))
            return pb2.UpdateRuntimeGlobalsResponse(
                ok=False,
                code_version=request.code_version,
                runtime_key=request.runtime_key or request.code_version,
                error=_err(pb2.ERROR_CODE_INVALID_REQUEST, str(exc)),
            )

    def GetMetrics(self, request: pb2.GetMetricsRequest, context: grpc.ServicerContext) -> pb2.GetMetricsResponse:
        logger.info("[NodeControl] GetMetrics peer=%s", _peer(context))
        metrics = self._state.metrics()
        return pb2.GetMetricsResponse(
            ok=True,
            node_id=self._state.node_id,
            queued=metrics["queued"],
            inflight=metrics["inflight"],
            running=metrics["running"],
            credit=metrics["credit"],
            queue_capacity=metrics["queue_capacity"],
            worker_capacity=metrics["worker_capacity"],
            cpu_percent=0.0,
            mem_percent=0.0,
            uptime_sec=metrics["uptime_sec"],
        )

    def CreateTaskPool(
        self,
        request_iterator: Iterable[pb2.CreateTaskPoolRequest],
        context: grpc.ServicerContext,
    ) -> pb2.CreateTaskPoolResponse:
        meta = None
        chunk_count = 0
        tmp_file = None
        tmp_path = ""
        for req in request_iterator:
            kind = req.WhichOneof("body")
            if kind == "meta":
                if meta is not None:
                    context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                    context.set_details("meta frame can only appear once")
                    return pb2.CreateTaskPoolResponse(ok=False, error=_err(pb2.ERROR_CODE_INVALID_REQUEST, "meta frame can only appear once"))
                meta = req.meta
            elif kind == "chunk":
                if meta is None:
                    context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                    context.set_details("meta frame must come before chunk frames")
                    return pb2.CreateTaskPoolResponse(ok=False, error=_err(pb2.ERROR_CODE_INVALID_REQUEST, "meta frame must come before chunk frames"))
                if tmp_file is None:
                    tmp_file = tempfile.NamedTemporaryFile(
                        mode="wb",
                        prefix="pycloud-task-pool-",
                        suffix=_tempfile_suffix_for_package_format(meta.package_format),
                        delete=False,
                        dir=str(self._state.artifact_dir),
                    )
                    tmp_path = tmp_file.name
                part = req.chunk or b""
                if part:
                    tmp_file.write(part)
                    chunk_count += 1

        if meta is None:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("missing task pool metadata frame")
            return pb2.CreateTaskPoolResponse(ok=False, error=_err(pb2.ERROR_CODE_INVALID_REQUEST, "missing task pool metadata frame"))

        if tmp_file is None:
            tmp_file = tempfile.NamedTemporaryFile(
                mode="wb",
                prefix="pycloud-task-pool-",
                suffix=".bin",
                delete=False,
                dir=str(self._state.artifact_dir),
            )
            tmp_path = tmp_file.name

        try:
            tmp_file.close()
            blob = Path(tmp_path).read_bytes()
            pool = self._state.create_task_pool(
                owner_client_id=meta.owner_client_id,
                pool_name=meta.pool_name,
                sha256=meta.sha256,
                runtime=meta.runtime,
                entry_module=meta.entry_module,
                entry_callable=meta.entry_callable,
                package_format=meta.package_format,
                dependency_policy_mode=meta.dependency_policy_mode,
                dependency_allowlist=list(meta.dependency_allowlist),
                managed_global_names=list(meta.managed_global_names),
                worker_count=meta.worker_count,
                heartbeat_timeout_sec=meta.heartbeat_timeout_sec,
                idle_ttl_sec=meta.idle_ttl_sec,
                chunks=[blob],
            )
        except Exception as exc:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(exc))
            return pb2.CreateTaskPoolResponse(ok=False, error=_err(pb2.ERROR_CODE_INVALID_REQUEST, str(exc)))
        finally:
            if tmp_path:
                try:
                    os.unlink(tmp_path)
                except FileNotFoundError:
                    pass

        return pb2.CreateTaskPoolResponse(
            ok=True,
            pool_id=pool.pool_id,
            code_version=pool.code_version,
            worker_count=pool.worker_count,
            heartbeat_timeout_sec=pool.heartbeat_timeout_sec,
            owner_client_id=pool.owner_client_id,
            pool_token=pool.pool_token,
        )

    def SubmitPoolTasks(self, request: pb2.SubmitPoolTasksRequest, context: grpc.ServicerContext) -> pb2.SubmitTasksResponse:
        log_payload_flow(
            "taskpool_submit_rpc",
            peer=_peer(context),
            pool_id=request.pool_id,
            task_count=len(request.tasks),
            job_id=(request.job_id or ""),
        )
        try:
            accepted, rejected = self._state.submit_pool_tasks(
                pool_id=request.pool_id,
                pool_token=request.pool_token,
                tasks=list(request.tasks),
                job_id=request.job_id,
            )
            log_payload_flow(
                "taskpool_submit_rpc_result",
                peer=_peer(context),
                pool_id=request.pool_id,
                accepted=len(accepted),
                rejected=len(rejected),
            )
            return pb2.SubmitTasksResponse(ok=True, accepted=accepted, rejected=rejected, node_credit=0)
        except KeyError as exc:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(str(exc))
            return pb2.SubmitTasksResponse(ok=False, error=_err(pb2.ERROR_CODE_TASK_NOT_FOUND, str(exc)))
        except PermissionError as exc:
            context.set_code(grpc.StatusCode.PERMISSION_DENIED)
            context.set_details(str(exc))
            return pb2.SubmitTasksResponse(ok=False, error=_err(pb2.ERROR_CODE_UNAUTHORIZED, str(exc)))
        except ValueError as exc:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(exc))
            return pb2.SubmitTasksResponse(ok=False, error=_err(pb2.ERROR_CODE_INVALID_REQUEST, str(exc)))

    def HeartbeatTaskPool(self, request: pb2.HeartbeatTaskPoolRequest, context: grpc.ServicerContext) -> pb2.HeartbeatTaskPoolResponse:
        try:
            pool = self._state.heartbeat_task_pool(
                owner_client_id=request.owner_client_id,
                pool_id=request.pool_id,
                pool_token=request.pool_token,
            )
            return pb2.HeartbeatTaskPoolResponse(
                ok=True,
                accepted=True,
                next_heartbeat_in_sec=max(1, pool.heartbeat_timeout_sec // 2),
            )
        except KeyError as exc:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(str(exc))
            return pb2.HeartbeatTaskPoolResponse(ok=False, accepted=False, error=_err(pb2.ERROR_CODE_TASK_NOT_FOUND, str(exc)))
        except PermissionError as exc:
            context.set_code(grpc.StatusCode.PERMISSION_DENIED)
            context.set_details(str(exc))
            return pb2.HeartbeatTaskPoolResponse(ok=False, accepted=False, error=_err(pb2.ERROR_CODE_UNAUTHORIZED, str(exc)))
        except RuntimeError as exc:
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            context.set_details(str(exc))
            return pb2.HeartbeatTaskPoolResponse(ok=False, accepted=False, error=_err(pb2.ERROR_CODE_INTERNAL_ERROR, str(exc)))

    def PullPoolResults(self, request: pb2.PullPoolResultsRequest, context: grpc.ServicerContext) -> pb2.PullResultsResponse:
        log_payload_flow(
            "taskpool_pull_results_rpc",
            peer=_peer(context),
            pool_id=request.pool_id,
            limit=max(1, int(request.limit or 100)),
            wait_ms=max(0, int(request.wait_ms or 0)),
            cursor=(request.cursor or ""),
        )
        try:
            results, next_cursor = self._state.pull_pool_results(
                pool_id=request.pool_id,
                pool_token=request.pool_token,
                limit=request.limit,
                wait_ms=request.wait_ms,
                cursor=request.cursor,
            )
            log_payload_flow(
                "taskpool_pull_results_rpc_result",
                peer=_peer(context),
                pool_id=request.pool_id,
                result_count=len(results),
                next_cursor=next_cursor,
            )
            return pb2.PullResultsResponse(ok=True, results=results, next_cursor=next_cursor)
        except KeyError as exc:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(str(exc))
            return pb2.PullResultsResponse(ok=False, error=_err(pb2.ERROR_CODE_TASK_NOT_FOUND, str(exc)))
        except PermissionError as exc:
            context.set_code(grpc.StatusCode.PERMISSION_DENIED)
            context.set_details(str(exc))
            return pb2.PullResultsResponse(ok=False, error=_err(pb2.ERROR_CODE_UNAUTHORIZED, str(exc)))

    def CancelPoolJob(self, request: pb2.CancelPoolJobRequest, context: grpc.ServicerContext) -> pb2.CancelJobResponse:
        try:
            queued_cancelled, running_marked, already_done, not_found = self._state.cancel_pool_job(
                pool_id=request.pool_id,
                pool_token=request.pool_token,
                job_id=request.job_id,
                reason=request.reason,
            )
            return pb2.CancelJobResponse(
                ok=True,
                queued_cancelled=queued_cancelled,
                running_marked=running_marked,
                already_done=already_done,
                not_found=not_found,
            )
        except KeyError as exc:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(str(exc))
            return pb2.CancelJobResponse(ok=False, error=_err(pb2.ERROR_CODE_TASK_NOT_FOUND, str(exc)))
        except PermissionError as exc:
            context.set_code(grpc.StatusCode.PERMISSION_DENIED)
            context.set_details(str(exc))
            return pb2.CancelJobResponse(ok=False, error=_err(pb2.ERROR_CODE_UNAUTHORIZED, str(exc)))
        except ValueError as exc:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(exc))
            return pb2.CancelJobResponse(ok=False, error=_err(pb2.ERROR_CODE_INVALID_REQUEST, str(exc)))

    def GetTaskPoolStatus(self, request: pb2.GetTaskPoolStatusRequest, context: grpc.ServicerContext) -> pb2.GetTaskPoolStatusResponse:
        try:
            pool = self._state.task_pool(request.pool_id)
            if pool.pool_token != str(request.pool_token or "").strip():
                raise PermissionError("pool_token mismatch")
            info = self._state.task_pool_status_info(request.pool_id)
            return pb2.GetTaskPoolStatusResponse(
                ok=True,
                pool=pb2.TaskPoolStatusInfo(
                    pool_id=str(info.get("pool_id", "")),
                    owner_client_id=str(info.get("owner_client_id", "")),
                    pool_name=str(info.get("pool_name", "")),
                    code_version=str(info.get("code_version", "")),
                    worker_count=int(info.get("worker_count", 0) or 0),
                    heartbeat_timeout_sec=int(info.get("heartbeat_timeout_sec", 0) or 0),
                    status=str(info.get("status", "")),
                    task_count=int(info.get("task_count", 0) or 0),
                    created_at=dt_to_ts(info["created_at"]),
                    last_heartbeat_at=dt_to_ts(info["last_heartbeat_at"]),
                    lease_expire_at=dt_to_ts(info["lease_expire_at"]),
                ),
            )
        except KeyError as exc:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(str(exc))
            return pb2.GetTaskPoolStatusResponse(ok=False, error=_err(pb2.ERROR_CODE_TASK_NOT_FOUND, str(exc)))
        except PermissionError as exc:
            context.set_code(grpc.StatusCode.PERMISSION_DENIED)
            context.set_details(str(exc))
            return pb2.GetTaskPoolStatusResponse(ok=False, error=_err(pb2.ERROR_CODE_UNAUTHORIZED, str(exc)))

    def CloseTaskPool(self, request: pb2.CloseTaskPoolRequest, context: grpc.ServicerContext) -> pb2.CloseTaskPoolResponse:
        try:
            self._state.close_task_pool(
                owner_client_id=request.owner_client_id,
                pool_id=request.pool_id,
                pool_token=request.pool_token,
                reason=request.reason,
            )
            return pb2.CloseTaskPoolResponse(ok=True, accepted=True)
        except KeyError as exc:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(str(exc))
            return pb2.CloseTaskPoolResponse(ok=False, accepted=False, error=_err(pb2.ERROR_CODE_TASK_NOT_FOUND, str(exc)))
        except PermissionError as exc:
            context.set_code(grpc.StatusCode.PERMISSION_DENIED)
            context.set_details(str(exc))
            return pb2.CloseTaskPoolResponse(ok=False, accepted=False, error=_err(pb2.ERROR_CODE_UNAUTHORIZED, str(exc)))

    def CreateService(
        self,
        request_iterator: Iterable[pb2.CreateServiceRequest],
        context: grpc.ServicerContext,
    ) -> pb2.CreateServiceResponse:
        meta = None
        chunk_count = 0
        tmp_file = None
        tmp_path = ""
        for req in request_iterator:
            kind = req.WhichOneof("body")
            if kind == "meta":
                if meta is not None:
                    context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                    context.set_details("meta frame can only appear once")
                    return pb2.CreateServiceResponse(
                        ok=False,
                        status=pb2.SERVICE_STATUS_UNSPECIFIED,
                        error=_err(pb2.ERROR_CODE_INVALID_REQUEST, "meta frame can only appear once"),
                    )
                meta = req.meta
            elif kind == "chunk":
                if meta is None:
                    context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                    context.set_details("meta frame must come before chunk frames")
                    return pb2.CreateServiceResponse(
                        ok=False,
                        status=pb2.SERVICE_STATUS_UNSPECIFIED,
                        error=_err(pb2.ERROR_CODE_INVALID_REQUEST, "meta frame must come before chunk frames"),
                    )
                if tmp_file is None:
                    tmp_file = tempfile.NamedTemporaryFile(
                        mode="wb",
                        prefix="pycloud-service-",
                        suffix=_tempfile_suffix_for_package_format(meta.package_format),
                        delete=False,
                        dir=str(self._state.artifact_dir),
                    )
                    tmp_path = tmp_file.name
                part = req.chunk or b""
                if part:
                    tmp_file.write(part)
                    chunk_count += 1

        logger.info(
            "[NodeControl] CreateService peer=%s owner_client_id=%s service_name=%s chunks=%d",
            _peer(context),
            (meta.owner_client_id if meta is not None else ""),
            (meta.service_name if meta is not None else ""),
            chunk_count,
        )

        if meta is None:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("missing service metadata frame")
            logger.warning("[NodeControl] CreateService missing meta peer=%s", _peer(context))
            return pb2.CreateServiceResponse(
                ok=False,
                status=pb2.SERVICE_STATUS_UNSPECIFIED,
                error=_err(pb2.ERROR_CODE_INVALID_REQUEST, "missing service metadata frame"),
            )

        if tmp_file is None:
            tmp_file = tempfile.NamedTemporaryFile(
                mode="wb",
                prefix="pycloud-service-",
                suffix=".bin",
                delete=False,
                dir=str(self._state.artifact_dir),
            )
            tmp_path = tmp_file.name

        try:
            tmp_file.close()
            export_spec = meta.export_spec

            def _iter_chunks(path: str, chunk_size: int = OBJECT_CHUNK_SIZE_BYTES):
                with open(path, "rb") as fp:
                    while True:
                        part = fp.read(max(1, int(chunk_size)))
                        if not part:
                            break
                        yield part

            session = self._state.create_service(
                owner_client_id=meta.owner_client_id,
                service_name=meta.service_name,
                sha256=meta.sha256,
                runtime=meta.runtime,
                entry_module=meta.entry_module,
                entry_callable=meta.entry_callable,
                package_format=meta.package_format,
                export_mode=export_spec.mode,
                export_methods=list(export_spec.methods),
                export_decorator=export_spec.decorator,
                dependency_policy_mode=meta.dependency_policy_mode,
                dependency_allowlist=list(meta.dependency_allowlist),
                managed_global_names=list(meta.managed_global_names),
                worker_count=meta.worker_count,
                heartbeat_timeout_sec=meta.heartbeat_timeout_sec,
                idle_ttl_sec=meta.idle_ttl_sec,
                expose_http=meta.expose_http,
                chunks=_iter_chunks(tmp_path),
            )
        except ValueError as exc:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(exc))
            logger.warning("[NodeControl] CreateService invalid request peer=%s err=%s", _peer(context), str(exc))
            return pb2.CreateServiceResponse(
                ok=False,
                status=pb2.SERVICE_STATUS_UNSPECIFIED,
                error=_err(pb2.ERROR_CODE_INVALID_REQUEST, str(exc)),
            )
        except Exception as exc:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(repr(exc))
            logger.exception("[NodeControl] CreateService internal error peer=%s", _peer(context))
            return pb2.CreateServiceResponse(
                ok=False,
                status=pb2.SERVICE_STATUS_UNSPECIFIED,
                error=_err(pb2.ERROR_CODE_INTERNAL_ERROR, repr(exc)),
            )
        finally:
            if tmp_path:
                try:
                    os.unlink(tmp_path)
                except FileNotFoundError:
                    pass

        self._notify_service_routes_changed()
        return pb2.CreateServiceResponse(
            ok=True,
            service_id=session.service_id,
            code_version=session.code_version,
            status=session.status,
            worker_count=session.worker_count,
            heartbeat_timeout_sec=session.heartbeat_timeout_sec,
            owner_client_id=session.owner_client_id,
            service_token=session.service_token,
            http_base_url=session.http_base_url,
        )

    def ListServiceMethods(
        self,
        request: pb2.ListServiceMethodsRequest,
        context: grpc.ServicerContext,
    ) -> pb2.ListServiceMethodsResponse:
        logger.info("[NodeControl] ListServiceMethods peer=%s service_id=%s", _peer(context), request.service_id)
        if not request.service_id:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("service_id is required")
            return pb2.ListServiceMethodsResponse(
                ok=False,
                error=_err(pb2.ERROR_CODE_INVALID_REQUEST, "service_id is required"),
            )
        try:
            methods = self._state.list_service_methods(request.service_id)
        except KeyError:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details("service not found")
            return pb2.ListServiceMethodsResponse(
                ok=False,
                error=_err(pb2.ERROR_CODE_TASK_NOT_FOUND, "service not found"),
            )

        out = []
        for item in methods:
            out.append(
                pb2.ServiceMethodInfo(
                    method=item["method"],
                    qualified_name=item["qualified_name"],
                    doc=item["doc"] if request.include_docs else "",
                )
            )
        return pb2.ListServiceMethodsResponse(ok=True, service_id=request.service_id, methods=out)

    def CallService(
        self,
        request: pb2.CallServiceRequest,
        context: grpc.ServicerContext,
    ) -> pb2.CallServiceResponse:
        logger.info(
            "[NodeControl] CallService peer=%s service_id=%s method=%s",
            _peer(context),
            request.service_id,
            request.method,
        )
        if not request.service_id or not request.method:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("service_id and method are required")
            return pb2.CallServiceResponse(
                ok=False,
                service_id=request.service_id,
                method=request.method,
                error=_err(pb2.ERROR_CODE_INVALID_REQUEST, "service_id and method are required"),
            )
        try:
            validate_inline_payload_structs(
                [request.payload],
                item_context="service call payload",
                request_context="call service request",
            )
        except ValueError as exc:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(exc))
            return pb2.CallServiceResponse(
                ok=False,
                service_id=request.service_id,
                method=request.method,
                error=_err(pb2.ERROR_CODE_INVALID_REQUEST, str(exc)),
            )

        code, body = self._state.call_service(
            service_id=request.service_id,
            method=request.method,
            payload=decode_payload_from_transport(
                struct_to_dict(request.payload),
                policy=get_payload_policy("http_call"),
            ),
            service_token=request.service_token,
            timeout_sec=max(0.1, float(request.timeout_sec or 60.0)),
        )
        if code == 404:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(str(body.get("error", "service/method not found")))
            return pb2.CallServiceResponse(
                ok=False,
                service_id=request.service_id,
                method=request.method,
                error=_err(pb2.ERROR_CODE_TASK_NOT_FOUND, str(body.get("error", "service/method not found"))),
            )
        if code == 401:
            context.set_code(grpc.StatusCode.PERMISSION_DENIED)
            context.set_details(str(body.get("error", "unauthorized")))
            return pb2.CallServiceResponse(
                ok=False,
                service_id=request.service_id,
                method=request.method,
                error=_err(pb2.ERROR_CODE_UNAUTHORIZED, str(body.get("error", "unauthorized"))),
            )
        if code >= 500:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(body.get("error", "internal error")))
            return pb2.CallServiceResponse(
                ok=False,
                service_id=request.service_id,
                method=request.method,
                error=_err(pb2.ERROR_CODE_INTERNAL_ERROR, str(body.get("error", "internal error"))),
            )
        if not body.get("ok", False):
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            context.set_details(str(body.get("error", "call rejected")))
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
        return pb2.CallServiceResponse(
            ok=True,
            service_id=request.service_id,
            method=request.method,
            data=dict_to_struct(body.get("data", {})),
        )

    def UpdateServiceGlobals(
        self,
        request: pb2.UpdateServiceGlobalsRequest,
        context: grpc.ServicerContext,
    ) -> pb2.UpdateServiceGlobalsResponse:
        logger.info(
            "[NodeControl] UpdateServiceGlobals peer=%s service_id=%s owner_client_id=%s",
            _peer(context),
            request.service_id,
            request.owner_client_id,
        )
        try:
            globals_digest, updated_names = self._state.update_service_globals(
                owner_client_id=request.owner_client_id,
                service_id=request.service_id,
                service_token=request.service_token,
                values=decode_payload_from_transport(
                    struct_to_dict(request.values),
                    policy=get_payload_policy("managed_globals"),
                ),
            )
            return pb2.UpdateServiceGlobalsResponse(
                ok=True,
                service_id=request.service_id,
                globals_digest=globals_digest,
                updated_names=updated_names,
            )
        except KeyError as exc:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(str(exc))
            return pb2.UpdateServiceGlobalsResponse(
                ok=False,
                service_id=request.service_id,
                error=_err(pb2.ERROR_CODE_TASK_NOT_FOUND, str(exc)),
            )
        except PermissionError as exc:
            context.set_code(grpc.StatusCode.PERMISSION_DENIED)
            context.set_details(str(exc))
            return pb2.UpdateServiceGlobalsResponse(
                ok=False,
                service_id=request.service_id,
                error=_err(pb2.ERROR_CODE_UNAUTHORIZED, str(exc)),
            )
        except ValueError as exc:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(exc))
            return pb2.UpdateServiceGlobalsResponse(
                ok=False,
                service_id=request.service_id,
                error=_err(pb2.ERROR_CODE_INVALID_REQUEST, str(exc)),
            )

    def HeartbeatService(
        self,
        request: pb2.HeartbeatServiceRequest,
        context: grpc.ServicerContext,
    ) -> pb2.HeartbeatServiceResponse:
        logger.info(
            "[NodeControl] HeartbeatService peer=%s owner_client_id=%s service_id=%s seq=%d",
            _peer(context),
            request.owner_client_id,
            request.service_id,
            int(request.seq),
        )
        if not request.owner_client_id or not request.service_id or not request.service_token:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("owner_client_id, service_id and service_token are required")
            logger.warning("[NodeControl] HeartbeatService invalid request peer=%s", _peer(context))
            return pb2.HeartbeatServiceResponse(
                ok=False,
                accepted=False,
                status=pb2.SERVICE_STATUS_UNSPECIFIED,
                error=_err(pb2.ERROR_CODE_INVALID_REQUEST, "owner_client_id, service_id and service_token are required"),
            )
        try:
            session = self._state.heartbeat_service(
                owner_client_id=request.owner_client_id,
                service_id=request.service_id,
                service_token=request.service_token,
            )
        except KeyError:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details("service not found")
            logger.warning(
                "[NodeControl] HeartbeatService service not found peer=%s service_id=%s",
                _peer(context),
                request.service_id,
            )
            return pb2.HeartbeatServiceResponse(
                ok=False,
                accepted=False,
                status=pb2.SERVICE_STATUS_UNSPECIFIED,
                error=_err(pb2.ERROR_CODE_TASK_NOT_FOUND, "service not found"),
            )
        except PermissionError as exc:
            context.set_code(grpc.StatusCode.PERMISSION_DENIED)
            context.set_details(str(exc))
            logger.warning("[NodeControl] HeartbeatService unauthorized peer=%s err=%s", _peer(context), str(exc))
            return pb2.HeartbeatServiceResponse(
                ok=False,
                accepted=False,
                status=pb2.SERVICE_STATUS_UNSPECIFIED,
                error=_err(pb2.ERROR_CODE_UNAUTHORIZED, str(exc)),
            )
        except RuntimeError as exc:
            logger.warning("[NodeControl] HeartbeatService runtime reject peer=%s err=%s", _peer(context), str(exc))
            return pb2.HeartbeatServiceResponse(
                ok=False,
                accepted=False,
                status=pb2.SERVICE_STATUS_STOPPED,
                error=_err(pb2.ERROR_CODE_NODE_DRAINING, str(exc)),
            )

        self._notify_service_routes_changed()
        return pb2.HeartbeatServiceResponse(
            ok=True,
            accepted=True,
            status=session.status,
            next_heartbeat_in_sec=max(1, session.heartbeat_timeout_sec // 2),
        )

    def EndService(
        self,
        request: pb2.EndServiceRequest,
        context: grpc.ServicerContext,
    ) -> pb2.EndServiceResponse:
        logger.info(
            "[NodeControl] EndService peer=%s owner_client_id=%s service_id=%s reason=%s",
            _peer(context),
            request.owner_client_id,
            request.service_id,
            request.reason,
        )
        if not request.owner_client_id or not request.service_id or not request.service_token:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("owner_client_id, service_id and service_token are required")
            logger.warning("[NodeControl] EndService invalid request peer=%s", _peer(context))
            return pb2.EndServiceResponse(
                ok=False,
                accepted=False,
                status=pb2.SERVICE_STATUS_UNSPECIFIED,
                error=_err(pb2.ERROR_CODE_INVALID_REQUEST, "owner_client_id, service_id and service_token are required"),
            )
        try:
            session = self._state.end_service(
                owner_client_id=request.owner_client_id,
                service_id=request.service_id,
                service_token=request.service_token,
                reason=request.reason,
            )
        except KeyError:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details("service not found")
            logger.warning(
                "[NodeControl] EndService service not found peer=%s service_id=%s",
                _peer(context),
                request.service_id,
            )
            return pb2.EndServiceResponse(
                ok=False,
                accepted=False,
                status=pb2.SERVICE_STATUS_UNSPECIFIED,
                error=_err(pb2.ERROR_CODE_TASK_NOT_FOUND, "service not found"),
            )
        except PermissionError as exc:
            context.set_code(grpc.StatusCode.PERMISSION_DENIED)
            context.set_details(str(exc))
            logger.warning("[NodeControl] EndService unauthorized peer=%s err=%s", _peer(context), str(exc))
            return pb2.EndServiceResponse(
                ok=False,
                accepted=False,
                status=pb2.SERVICE_STATUS_UNSPECIFIED,
                error=_err(pb2.ERROR_CODE_UNAUTHORIZED, str(exc)),
            )

        self._notify_service_routes_changed()
        return pb2.EndServiceResponse(ok=True, accepted=True, status=session.status)

    def GetServiceStatus(
        self,
        request: pb2.GetServiceStatusRequest,
        context: grpc.ServicerContext,
    ) -> pb2.GetServiceStatusResponse:
        logger.info("[NodeControl] GetServiceStatus peer=%s service_id=%s", _peer(context), request.service_id)
        if not request.service_id:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("service_id is required")
            logger.warning("[NodeControl] GetServiceStatus invalid request peer=%s", _peer(context))
            return pb2.GetServiceStatusResponse(
                ok=False,
                error=_err(pb2.ERROR_CODE_INVALID_REQUEST, "service_id is required"),
            )
        try:
            info = self._state.service_status_info(request.service_id)
        except KeyError:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details("service not found")
            logger.warning(
                "[NodeControl] GetServiceStatus service not found peer=%s service_id=%s",
                _peer(context),
                request.service_id,
            )
            return pb2.GetServiceStatusResponse(
                ok=False,
                error=_err(pb2.ERROR_CODE_TASK_NOT_FOUND, "service not found"),
            )
        return pb2.GetServiceStatusResponse(ok=True, service=_service_info_to_pb(info))
