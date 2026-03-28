from __future__ import annotations

"""Server bootstrap for PyCloud control-plane gRPC services."""

import argparse
import signal
from concurrent import futures
from typing import Callable, Tuple

import grpc

from pycloud_parallel.controlplane.services import InfoCenterService, NodeControlService, WorkerInternalService
from pycloud_parallel.controlplane.state import InfoCenterState, NodeControlState
from pycloud_parallel.grpc.v1 import pycloud_v1_pb2_grpc as pb2_grpc


def build_infocenter_server(bind: str, *, max_workers: int = 32) -> grpc.Server:
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=max(1, max_workers)))
    state = InfoCenterState()
    pb2_grpc.add_InfoCenterServiceServicer_to_server(InfoCenterService(state), server)
    server.add_insecure_port(bind)
    return server


def build_nodecontrol_server(
    bind: str,
    *,
    node_id: str,
    worker_capacity: int = 32,
    queue_capacity: int = 4000,
    max_workers: int = 64,
) -> Tuple[grpc.Server, NodeControlState]:
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=max(1, max_workers)))
    state = NodeControlState(
        node_id=node_id,
        worker_capacity=worker_capacity,
        queue_capacity=queue_capacity,
    )
    pb2_grpc.add_NodeControlServiceServicer_to_server(NodeControlService(state), server)
    pb2_grpc.add_WorkerInternalServiceServicer_to_server(WorkerInternalService(state), server)
    server.add_insecure_port(bind)
    return server, state


def _wait_until_stopped(server: grpc.Server, on_stop: Callable[[], None]) -> None:
    server.start()
    stop_called = False

    def _graceful_stop(*_args):
        nonlocal stop_called
        if stop_called:
            return
        stop_called = True
        on_stop()
        server.stop(grace=3)

    # Windows compatibility: not every signal is always available.
    for sig_name in ("SIGINT", "SIGTERM"):
        sig = getattr(signal, sig_name, None)
        if sig is None:
            continue
        try:
            signal.signal(sig, _graceful_stop)
        except (ValueError, OSError):
            # ValueError: not in main thread
            # OSError: unsupported signal in current platform/runtime
            continue
    server.wait_for_termination()


def main() -> None:
    parser = argparse.ArgumentParser(description="PyCloud gRPC control-plane server")
    parser.add_argument("--role", choices=["infocenter", "nodecontrol"], required=True)
    parser.add_argument("--bind", default="0.0.0.0:50051")
    parser.add_argument("--node-id", default="node-local-01")
    parser.add_argument("--queue-capacity", type=int, default=4000)
    parser.add_argument("--worker-capacity", type=int, default=32)
    parser.add_argument("--max-workers", type=int, default=64)
    args = parser.parse_args()

    if args.role == "infocenter":
        server = build_infocenter_server(args.bind, max_workers=args.max_workers)
        _wait_until_stopped(server, on_stop=lambda: None)
        return

    server, state = build_nodecontrol_server(
        args.bind,
        node_id=args.node_id,
        queue_capacity=args.queue_capacity,
        worker_capacity=args.worker_capacity,
        max_workers=args.max_workers,
    )
    _wait_until_stopped(server, on_stop=state.close)


if __name__ == "__main__":
    main()
