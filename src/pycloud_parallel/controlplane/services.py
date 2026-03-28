from __future__ import annotations

"""gRPC service implementations for PyCloud control-plane."""

from typing import Iterable, List

import grpc

from pycloud_parallel.controlplane.state import InfoCenterState, NodeControlState, dt_to_ts
from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2
from pycloud_parallel.grpc.v1 import pycloud_v1_pb2_grpc as pb2_grpc


def _err(code: int, message: str, request_id: str = "") -> pb2.Error:
    return pb2.Error(code=code, message=message, request_id=request_id)


class InfoCenterService(pb2_grpc.InfoCenterServiceServicer):
    def __init__(self, state: InfoCenterState) -> None:
        self._state = state

    def RegisterNode(self, request: pb2.RegisterNodeRequest, context: grpc.ServicerContext) -> pb2.RegisterNodeResponse:
        if not request.node_id or not request.control_addr:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("node_id and control_addr are required")
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
        if not request.node_id:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("node_id is required")
            return pb2.HeartbeatNodeResponse(
                ok=False,
                accepted=False,
                error=_err(pb2.ERROR_CODE_INVALID_REQUEST, "node_id is required"),
            )
        node = self._state.heartbeat(request)
        if node is None:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details("unknown node")
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
        return pb2.ListNodesResponse(ok=True, nodes=out)


class NodeControlService(pb2_grpc.NodeControlServiceServicer):
    def __init__(self, state: NodeControlState) -> None:
        self._state = state

    def UploadCode(self, request_iterator: Iterable[pb2.UploadCodeRequest], context: grpc.ServicerContext) -> pb2.UploadCodeResponse:
        meta = None
        chunks: List[bytes] = []
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
                chunks.append(req.chunk)

        if meta is None:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("missing upload metadata frame")
            return pb2.UploadCodeResponse(
                ok=False,
                error=_err(pb2.ERROR_CODE_INVALID_REQUEST, "missing upload metadata frame"),
            )

        try:
            artifact, cached = self._state.put_code(
                sha256=meta.sha256,
                filename=meta.filename or "artifact.bin",
                chunks=chunks,
            )
        except ValueError as exc:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(exc))
            return pb2.UploadCodeResponse(
                ok=False,
                error=_err(pb2.ERROR_CODE_INVALID_REQUEST, str(exc)),
            )

        return pb2.UploadCodeResponse(
            ok=True,
            code_version=artifact.code_version,
            cached=cached,
            size_bytes=artifact.size_bytes,
            created_at=dt_to_ts(artifact.created_at),
        )

    def SubmitTasks(self, request: pb2.SubmitTasksRequest, context: grpc.ServicerContext) -> pb2.SubmitTasksResponse:
        if not request.client_id:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("client_id is required")
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
        return pb2.SubmitTasksResponse(ok=True, accepted=accepted, rejected=rejected, node_credit=credit)

    def PullResults(self, request: pb2.PullResultsRequest, context: grpc.ServicerContext) -> pb2.PullResultsResponse:
        if not request.client_id:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("client_id is required")
            return pb2.PullResultsResponse(
                ok=False,
                error=_err(pb2.ERROR_CODE_INVALID_REQUEST, "client_id is required"),
            )
        results, next_cursor = self._state.pull_results(request)
        return pb2.PullResultsResponse(ok=True, results=results, next_cursor=next_cursor)

    def CancelTasks(self, request: pb2.CancelTasksRequest, context: grpc.ServicerContext) -> pb2.CancelTasksResponse:
        if not request.client_id:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("client_id is required")
            return pb2.CancelTasksResponse(
                ok=False,
                error=_err(pb2.ERROR_CODE_INVALID_REQUEST, "client_id is required"),
            )
        cancelled, not_found, already_done = self._state.cancel_tasks(request)
        return pb2.CancelTasksResponse(
            ok=True,
            cancelled=cancelled,
            not_found=not_found,
            already_done=already_done,
        )

    def GetMetrics(self, request: pb2.GetMetricsRequest, context: grpc.ServicerContext) -> pb2.GetMetricsResponse:
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


class WorkerInternalService(pb2_grpc.WorkerInternalServiceServicer):
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

