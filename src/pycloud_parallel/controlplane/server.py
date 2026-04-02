from __future__ import annotations

"""Server bootstrap for PyCloud control-plane services."""

import argparse
import logging
import signal
from concurrent import futures
from typing import Callable, Optional, Tuple

import grpc

from pycloud_parallel.controlplane.gateway_cache import GatewayRouteCache
from pycloud_parallel.controlplane.gateway_http import GatewayHttpApp, GatewayHttpServer
from pycloud_parallel.controlplane.gateway_source import InProcessInfoCenterSource, RemoteInfoCenterSource
from pycloud_parallel.controlplane.infocenter_http import InfoCenterHttpServer
from pycloud_parallel.controlplane.registrar import NodeInfoCenterRegistrar
from pycloud_parallel.controlplane.services import NodeControlService
from pycloud_parallel.controlplane.state import InfoCenterState, NodeControlState
from pycloud_parallel.grpc.v1 import pycloud_v1_pb2_grpc as pb2_grpc

logger = logging.getLogger(__name__)


def build_infocenter_server(bind: str, *, max_workers: int = 32) -> InfoCenterHttpServer:
    del max_workers
    server = InfoCenterHttpServer(
        bind=bind,
        state=InfoCenterState(heartbeat_interval_sec=5),
    )
    return server


def build_controlplane_server(
    bind: str,
    *,
    gateway_refresh_interval_sec: float = 3.0,
    gateway_failure_threshold: int = 3,
    gateway_open_sec: float = 5.0,
) -> InfoCenterHttpServer:
    info_state = InfoCenterState(heartbeat_interval_sec=5)
    route_cache = GatewayRouteCache(
        source=InProcessInfoCenterSource(info_state),
        refresh_interval_sec=gateway_refresh_interval_sec,
        failure_threshold=gateway_failure_threshold,
        open_sec=gateway_open_sec,
    )
    gateway_app = GatewayHttpApp(route_cache=route_cache)
    return InfoCenterHttpServer(
        bind=bind,
        state=info_state,
        gateway_app=gateway_app,
    )


def build_gateway_server(
    bind: str,
    *,
    infocenter_addr: str,
    gateway_refresh_interval_sec: float = 3.0,
    gateway_failure_threshold: int = 3,
    gateway_open_sec: float = 5.0,
) -> GatewayHttpServer:
    if not infocenter_addr:
        raise ValueError("infocenter_addr is required for gateway role")
    route_cache = GatewayRouteCache(
        source=RemoteInfoCenterSource(infocenter_addr),
        refresh_interval_sec=gateway_refresh_interval_sec,
        failure_threshold=gateway_failure_threshold,
        open_sec=gateway_open_sec,
    )
    return GatewayHttpServer(bind=bind, app=GatewayHttpApp(route_cache=route_cache))


def build_nodecontrol_server(
    bind: str,
    *,
    node_id: str,
    worker_capacity: int = 32,
    queue_capacity: int = 4000,
    max_workers: int = 64,
    service_http_bind: str = "127.0.0.1:18080",
    service_http_base_url: str = "",
    service_default_worker_count: int = 10,
    service_default_heartbeat_timeout_sec: int = 30,
    on_service_routes_changed: Optional[Callable[[], None]] = None,
) -> Tuple[grpc.Server, NodeControlState]:
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=max(1, max_workers)))
    state = NodeControlState(
        node_id=node_id,
        worker_capacity=worker_capacity,
        queue_capacity=queue_capacity,
        service_http_bind=service_http_bind,
        service_http_base_url=service_http_base_url,
        service_default_worker_count=service_default_worker_count,
        service_default_heartbeat_timeout_sec=service_default_heartbeat_timeout_sec,
    )
    pb2_grpc.add_NodeControlServiceServicer_to_server(
        NodeControlService(state, on_service_routes_changed=on_service_routes_changed),
        server,
    )
    server.add_insecure_port(bind)
    return server, state


def _wait_until_stopped(
    server,
    on_stop: Callable[[], None],
    *,
    on_start: Optional[Callable[[], None]] = None,
) -> None:
    """等待服务器停止。

    设置信号处理器，在收到 SIGINT/SIGTERM 时优雅关闭服务器。

    Args:
        server: gRPC 服务器
        on_stop: 停止前调用的回调函数
    """
    server.start()
    if on_start is not None:
        on_start()
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
    """主函数。

    根据命令行参数启动 InfoCenter 或 NodeControl 服务器。
    """
    parser = argparse.ArgumentParser(description="PyCloud control-plane server")
    parser.add_argument("--role", choices=["infocenter", "gateway", "controlplane", "nodecontrol"], required=True)
    parser.add_argument("--bind", default="0.0.0.0:50051")
    parser.add_argument("--node-id", default="node-local-01")
    parser.add_argument("--queue-capacity", type=int, default=4000)
    parser.add_argument("--worker-capacity", type=int, default=32)
    parser.add_argument("--max-workers", type=int, default=64)
    parser.add_argument("--service-http-bind", default="127.0.0.1:18080")
    parser.add_argument("--service-http-base-url", default="")
    parser.add_argument("--service-default-workers", type=int, default=10)
    parser.add_argument("--service-heartbeat-timeout-sec", type=int, default=30)
    parser.add_argument("--infocenter-addr", default="")
    parser.add_argument("--advertise-addr", default="")
    parser.add_argument("--node-tags", default="compute")
    parser.add_argument("--node-version", default="v1")
    parser.add_argument("--gateway-refresh-interval-sec", type=float, default=3.0)
    parser.add_argument("--gateway-failure-threshold", type=int, default=3)
    parser.add_argument("--gateway-open-sec", type=float, default=5.0)
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    level_name = str(args.log_level or "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    if args.role == "infocenter":
        logger.info("[Server] starting InfoCenter bind=%s log_level=%s", args.bind, level_name)
        server = build_infocenter_server(args.bind, max_workers=args.max_workers)
        _wait_until_stopped(server, on_stop=lambda: None)
        return

    if args.role == "controlplane":
        logger.info("[Server] starting ControlPlane bind=%s log_level=%s", args.bind, level_name)
        server = build_controlplane_server(
            args.bind,
            gateway_refresh_interval_sec=args.gateway_refresh_interval_sec,
            gateway_failure_threshold=args.gateway_failure_threshold,
            gateway_open_sec=args.gateway_open_sec,
        )
        _wait_until_stopped(server, on_stop=lambda: None)
        return

    if args.role == "gateway":
        logger.info(
            "[Server] starting Gateway bind=%s infocenter=%s log_level=%s",
            args.bind,
            args.infocenter_addr,
            level_name,
        )
        server = build_gateway_server(
            args.bind,
            infocenter_addr=args.infocenter_addr,
            gateway_refresh_interval_sec=args.gateway_refresh_interval_sec,
            gateway_failure_threshold=args.gateway_failure_threshold,
            gateway_open_sec=args.gateway_open_sec,
        )
        _wait_until_stopped(server, on_stop=lambda: None)
        return

    logger.info(
        "[Server] starting NodeControl bind=%s node_id=%s infocenter=%s advertise=%s log_level=%s",
        args.bind,
        args.node_id,
        args.infocenter_addr,
        args.advertise_addr or args.bind,
        level_name,
    )
    advertise_addr = (args.advertise_addr or args.bind).strip()
    node_tags = [x.strip() for x in args.node_tags.split(",") if x.strip()]

    registrar_holder: dict[str, Optional[NodeInfoCenterRegistrar]] = {"value": None}

    def _sync_routes_now() -> None:
        registrar = registrar_holder["value"]
        if registrar is not None:
            registrar.sync_now()

    server, state = build_nodecontrol_server(
        args.bind,
        node_id=args.node_id,
        queue_capacity=args.queue_capacity,
        worker_capacity=args.worker_capacity,
        max_workers=args.max_workers,
        service_http_bind=args.service_http_bind,
        service_http_base_url=args.service_http_base_url,
        service_default_worker_count=args.service_default_workers,
        service_default_heartbeat_timeout_sec=args.service_heartbeat_timeout_sec,
        on_service_routes_changed=_sync_routes_now,
    )

    registrar: Optional[NodeInfoCenterRegistrar] = None
    if args.infocenter_addr:
        registrar = NodeInfoCenterRegistrar(
            infocenter_addr=args.infocenter_addr,
            node_id=args.node_id,
            control_addr=advertise_addr,
            state=state,
            capacity=args.worker_capacity,
            queue_capacity=args.queue_capacity,
            tags=node_tags,
            version=args.node_version,
            metadata={"role": "compute-node"},
        )
        registrar_holder["value"] = registrar

    def _on_start() -> None:
        if registrar is not None:
            logger.info(
                "[Server] NodeControl registrar start node_id=%s infocenter=%s advertise=%s",
                args.node_id,
                args.infocenter_addr,
                advertise_addr,
            )
            registrar.start()

    def _on_stop() -> None:
        if registrar is not None:
            logger.info("[Server] NodeControl registrar stop node_id=%s", args.node_id)
            registrar.close()
        state.close()

    _wait_until_stopped(server, on_stop=_on_stop, on_start=_on_start)


if __name__ == "__main__":
    main()
