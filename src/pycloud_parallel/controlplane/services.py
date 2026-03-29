from __future__ import annotations

"""gRPC service implementations for PyCloud control-plane."""

import logging
import hashlib
import os
import tempfile
from typing import Iterable, List

import grpc

from pycloud_parallel.controlplane.state import InfoCenterState, NodeControlState, dt_to_ts, struct_to_dict
from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2
from pycloud_parallel.grpc.v1 import pycloud_v1_pb2_grpc as pb2_grpc

logger = logging.getLogger(__name__)


def _err(code: int, message: str, request_id: str = "") -> pb2.Error:
    return pb2.Error(code=code, message=message, request_id=request_id)


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


class InfoCenterService(pb2_grpc.InfoCenterServiceServicer):
    def __init__(self, state: InfoCenterState) -> None:
        self._state = state

    def RegisterNode(self, request: pb2.RegisterNodeRequest, context: grpc.ServicerContext) -> pb2.RegisterNodeResponse:
        logger.info(
            "[InfoCenter] RegisterNode peer=%s node_id=%s control_addr=%s tags=%s services=%d",
            _peer(context),
            request.node_id,
            request.control_addr,
            list(request.tags),
            len(request.services),
        )
        if not request.node_id or not request.control_addr:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("node_id and control_addr are required")
            logger.warning("[InfoCenter] RegisterNode invalid request peer=%s", _peer(context))
            return pb2.RegisterNodeResponse(
                ok=False,
                error=_err(pb2.ERROR_CODE_INVALID_REQUEST, "node_id and control_addr are required"),
            )
        node = self._state.register_node(request)
        return pb2.RegisterNodeResponse(
            ok=True,
            node_id=node.node_id,
            lease_ttl_sec=self._state.lease_ttl_sec,
            heartbeat_interval_sec=self._state.heartbeat_interval_sec,
            server_time=dt_to_ts(node.last_seen_at),
        )

    def HeartbeatNode(self, request: pb2.HeartbeatNodeRequest, context: grpc.ServicerContext) -> pb2.HeartbeatNodeResponse:
        logger.info(
            "[InfoCenter] HeartbeatNode peer=%s node_id=%s healthy=%s queued=%d inflight=%d services=%d",
            _peer(context),
            request.node_id,
            bool(request.healthy),
            int(request.metrics.queued),
            int(request.metrics.inflight),
            len(request.services),
        )
        if not request.node_id:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("node_id is required")
            logger.warning("[InfoCenter] HeartbeatNode invalid request peer=%s", _peer(context))
            return pb2.HeartbeatNodeResponse(
                ok=False,
                accepted=False,
                error=_err(pb2.ERROR_CODE_INVALID_REQUEST, "node_id is required"),
            )
        node = self._state.heartbeat(request)
        if node is None:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details("unknown node")
            logger.warning(
                "[InfoCenter] HeartbeatNode unknown node peer=%s node_id=%s",
                _peer(context),
                request.node_id,
            )
            return pb2.HeartbeatNodeResponse(
                ok=False,
                accepted=False,
                error=_err(pb2.ERROR_CODE_UNKNOWN_NODE, "unknown node"),
            )
        return pb2.HeartbeatNodeResponse(
            ok=True,
            accepted=True,
            next_heartbeat_in_sec=self._state.heartbeat_interval_sec,
            drain=False,
        )

    def ListNodes(self, request: pb2.ListNodesRequest, context: grpc.ServicerContext) -> pb2.ListNodesResponse:
        logger.info(
            "[InfoCenter] ListNodes peer=%s healthy_only=%s tags=%s limit=%d",
            _peer(context),
            bool(request.healthy_only),
            list(request.tags),
            int(request.limit or 100),
        )
        nodes = self._state.list_nodes(
            healthy_only=bool(request.healthy_only),
            tags=request.tags,
            limit=request.limit or 100,
        )
        out = []
        for node in nodes:
            out.append(
                pb2.NodeInfo(
                    node_id=node.node_id,
                    control_addr=node.control_addr,
                    healthy=node.healthy,
                    last_seen_at=dt_to_ts(node.last_seen_at),
                    capacity=node.capacity,
                    queue_capacity=node.queue_capacity,
                    queued=node.metrics.queued,
                    inflight=node.metrics.inflight,
                    credit=node.metrics.credit,
                    tags=node.tags,
                )
            )
        logger.info("[InfoCenter] ListNodes result_count=%d", len(out))
        return pb2.ListNodesResponse(ok=True, nodes=out)

    def ListServiceRoutes(
        self,
        request: pb2.ListServiceRoutesRequest,
        context: grpc.ServicerContext,
    ) -> pb2.ListServiceRoutesResponse:
        logger.info(
            "[InfoCenter] ListServiceRoutes peer=%s service_name=%s healthy_only=%s limit=%d",
            _peer(context),
            request.service_name,
            bool(request.healthy_only),
            int(request.limit or 200),
        )
        routes = self._state.list_service_routes(
            service_name=request.service_name,
            healthy_only=bool(request.healthy_only),
            limit=request.limit or 200,
        )
        logger.info("[InfoCenter] ListServiceRoutes result_count=%d", len(routes))
        return pb2.ListServiceRoutesResponse(
            ok=True,
            routes=[_service_route_to_pb(item) for item in routes],
        )


class NodeControlService(pb2_grpc.NodeControlServiceServicer):
    """NodeControl gRPC 服务。

    负责代码上传、任务提交、结果拉取等核心功能。

    Attributes:
        _state: NodeControl 状态管理器
    """

    def __init__(self, state: NodeControlState) -> None:
        self._state = state

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
                    suffix = ".tar.gz" if str(meta.filename or "").lower().endswith(".tar.gz") else ""
                    tmp_file = tempfile.NamedTemporaryFile(
                        mode="wb",
                        prefix="pycloud-upload-",
                        suffix=suffix or ".bin",
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
            "[NodeControl] UploadCode peer=%s client_id=%s filename=%s chunks=%d",
            _peer(context),
            (meta.client_id if meta is not None else ""),
            (meta.filename if meta is not None else ""),
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
                sha256=meta.sha256,
                filename=meta.filename or "artifact.bin",
                runtime=meta.runtime,
                entry_module=meta.entry_module,
                entry_callable=meta.entry_callable,
                package_format=meta.package_format,
                export_mode=export_spec.mode,
                export_methods=list(export_spec.methods),
                export_decorator=export_spec.decorator,
                uploaded_path=tmp_path,
                actual_sha256=h.hexdigest(),
                size_bytes=size_bytes,
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
        )

    def SubmitTasks(self, request: pb2.SubmitTasksRequest, context: grpc.ServicerContext) -> pb2.SubmitTasksResponse:
        logger.info(
            "[NodeControl] SubmitTasks peer=%s client_id=%s code_version=%s tasks=%d",
            _peer(context),
            request.client_id,
            request.code_version,
            len(request.tasks),
        )
        if not request.client_id:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("client_id is required")
            logger.warning("[NodeControl] SubmitTasks invalid request peer=%s", _peer(context))
            return pb2.SubmitTasksResponse(
                ok=False,
                error=_err(pb2.ERROR_CODE_INVALID_REQUEST, "client_id is required"),
            )
        if not request.tasks:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("tasks cannot be empty")
            return pb2.SubmitTasksResponse(
                ok=False,
                error=_err(pb2.ERROR_CODE_INVALID_REQUEST, "tasks cannot be empty"),
            )
        accepted, rejected, credit = self._state.submit_tasks(request)
        logger.info(
            "[NodeControl] SubmitTasks result peer=%s accepted=%d rejected=%d credit=%d",
            _peer(context),
            len(accepted),
            len(rejected),
            int(credit),
        )
        return pb2.SubmitTasksResponse(ok=True, accepted=accepted, rejected=rejected, node_credit=credit)

    def PullResults(self, request: pb2.PullResultsRequest, context: grpc.ServicerContext) -> pb2.PullResultsResponse:
        logger.info(
            "[NodeControl] PullResults peer=%s client_id=%s limit=%d wait_ms=%d",
            _peer(context),
            request.client_id,
            int(request.limit or 100),
            int(request.wait_ms),
        )
        if not request.client_id:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("client_id is required")
            logger.warning("[NodeControl] PullResults invalid request peer=%s", _peer(context))
            return pb2.PullResultsResponse(
                ok=False,
                error=_err(pb2.ERROR_CODE_INVALID_REQUEST, "client_id is required"),
            )
        results, next_cursor = self._state.pull_results(request)
        logger.info(
            "[NodeControl] PullResults result peer=%s client_id=%s results=%d next_cursor=%s",
            _peer(context),
            request.client_id,
            len(results),
            next_cursor,
        )
        return pb2.PullResultsResponse(ok=True, results=results, next_cursor=next_cursor)

    def CancelTasks(self, request: pb2.CancelTasksRequest, context: grpc.ServicerContext) -> pb2.CancelTasksResponse:
        logger.info(
            "[NodeControl] CancelTasks peer=%s client_id=%s task_ids=%d",
            _peer(context),
            request.client_id,
            len(request.task_ids),
        )
        if not request.client_id:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("client_id is required")
            logger.warning("[NodeControl] CancelTasks invalid request peer=%s", _peer(context))
            return pb2.CancelTasksResponse(
                ok=False,
                error=_err(pb2.ERROR_CODE_INVALID_REQUEST, "client_id is required"),
            )
        cancelled, not_found, already_done = self._state.cancel_tasks(request)
        logger.info(
            "[NodeControl] CancelTasks result peer=%s cancelled=%d not_found=%d already_done=%d",
            _peer(context),
            len(cancelled),
            len(not_found),
            len(already_done),
        )
        return pb2.CancelTasksResponse(
            ok=True,
            cancelled=cancelled,
            not_found=not_found,
            already_done=already_done,
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
                    suffix = ".tar.gz" if str(meta.filename or "").lower().endswith(".tar.gz") else ""
                    tmp_file = tempfile.NamedTemporaryFile(
                        mode="wb",
                        prefix="pycloud-service-",
                        suffix=suffix or ".bin",
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

            def _iter_chunks(path: str, chunk_size: int = 256 * 1024):
                with open(path, "rb") as fp:
                    while True:
                        part = fp.read(max(1, int(chunk_size)))
                        if not part:
                            break
                        yield part

            session = self._state.create_service(
                owner_client_id=meta.owner_client_id,
                service_name=meta.service_name,
                filename=meta.filename or "service_artifact.py",
                sha256=meta.sha256,
                runtime=meta.runtime,
                entry_module=meta.entry_module,
                entry_callable=meta.entry_callable,
                package_format=meta.package_format,
                export_mode=export_spec.mode,
                export_methods=list(export_spec.methods),
                export_decorator=export_spec.decorator,
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

        code, body = self._state.call_service(
            service_id=request.service_id,
            method=request.method,
            payload=struct_to_dict(request.payload),
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
            data=body.get("data", {}),
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
        if not request.owner_client_id or not request.service_id:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("owner_client_id and service_id are required")
            logger.warning("[NodeControl] HeartbeatService invalid request peer=%s", _peer(context))
            return pb2.HeartbeatServiceResponse(
                ok=False,
                accepted=False,
                status=pb2.SERVICE_STATUS_UNSPECIFIED,
                error=_err(pb2.ERROR_CODE_INVALID_REQUEST, "owner_client_id and service_id are required"),
            )
        try:
            session = self._state.heartbeat_service(
                owner_client_id=request.owner_client_id,
                service_id=request.service_id,
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
        if not request.owner_client_id or not request.service_id:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("owner_client_id and service_id are required")
            logger.warning("[NodeControl] EndService invalid request peer=%s", _peer(context))
            return pb2.EndServiceResponse(
                ok=False,
                accepted=False,
                status=pb2.SERVICE_STATUS_UNSPECIFIED,
                error=_err(pb2.ERROR_CODE_INVALID_REQUEST, "owner_client_id and service_id are required"),
            )
        try:
            session = self._state.end_service(
                owner_client_id=request.owner_client_id,
                service_id=request.service_id,
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


class WorkerInternalService(pb2_grpc.WorkerInternalServiceServicer):
    """Worker 内部 gRPC 服务。

    供工作进程内部调用，负责任务轮询、心跳和结果上报。

    Attributes:
        _state: NodeControl 状态管理器
    """

    def __init__(self, state: NodeControlState) -> None:
        self._state = state

    def PollTask(self, request: pb2.PollTaskRequest, context: grpc.ServicerContext) -> pb2.PollTaskResponse:
        if not request.worker_id:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("worker_id is required")
            return pb2.PollTaskResponse(
                ok=False,
                error=_err(pb2.ERROR_CODE_INVALID_REQUEST, "worker_id is required"),
            )
        task = self._state.poll_task(request.worker_id)
        if task is None:
            return pb2.PollTaskResponse(ok=True, idle={})
        return pb2.PollTaskResponse(ok=True, task=task)

    def HeartbeatTask(self, request: pb2.HeartbeatTaskRequest, context: grpc.ServicerContext) -> pb2.HeartbeatTaskResponse:
        if not request.worker_id or not request.task_id:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("worker_id and task_id are required")
            return pb2.HeartbeatTaskResponse(
                ok=False,
                accepted=False,
                error=_err(pb2.ERROR_CODE_INVALID_REQUEST, "worker_id and task_id are required"),
            )
        accepted, cancel_requested = self._state.heartbeat_task(request)
        return pb2.HeartbeatTaskResponse(ok=True, accepted=accepted, cancel_requested=cancel_requested)

    def ReportResult(self, request: pb2.ReportResultRequest, context: grpc.ServicerContext) -> pb2.ReportResultResponse:
        if not request.worker_id or not request.task_id:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("worker_id and task_id are required")
            return pb2.ReportResultResponse(
                ok=False,
                accepted=False,
                error=_err(pb2.ERROR_CODE_INVALID_REQUEST, "worker_id and task_id are required"),
            )
        accepted = self._state.report_result(request)
        return pb2.ReportResultResponse(ok=True, accepted=accepted)
