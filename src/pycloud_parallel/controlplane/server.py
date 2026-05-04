from __future__ import annotations

"""Server bootstrap for PyCloud control-plane services."""

import argparse
import os
import logging
from pathlib import Path
import re
import signal
from typing import Callable, Optional, Tuple

from pycloud_parallel.controlplane.config import (
    NODE_MAX_WORKERS,
    NODE_QUEUE_CAPACITY,
    NODE_WORKER_CAPACITY,
    EXECUTOR_BACKEND,
    SERVICE_DEFAULT_WORKERS,
    SERVICE_HEARTBEAT_TIMEOUT_SEC,
)
from pycloud_parallel.controlplane.gateway_cache import GatewayRouteCache
from pycloud_parallel.controlplane.gateway_http import GatewayHttpApp, GatewayHttpServer
from pycloud_parallel.controlplane.gateway_source import InProcessInfoCenterSource, RemoteInfoCenterSource
from pycloud_parallel.controlplane.infocenter_http import InfoCenterHttpServer
from pycloud_parallel.controlplane.job_orchestrator import (
    DEFAULT_JOB_ORCHESTRATOR_SERVICE_NAME,
    JobOrchestratorServer,
)
from pycloud_parallel.controlplane.job_queue import JobQueueManager
from pycloud_parallel.controlplane.netutil import detect_local_ip, format_host_port, resolve_public_host, split_host_port
from pycloud_parallel.controlplane.node_control_http import NodeControlHttpServer
from pycloud_parallel.controlplane.registrar import NodeInfoCenterRegistrar
from pycloud_parallel.controlplane.infocenter_state import InfoCenterState
from pycloud_parallel.controlplane.nodecontrol_state import NodeControlState

logger = logging.getLogger(__name__)


def _safe_artifact_dir_part(value: str, *, fallback: str) -> str:
    text = re.sub(r"[^A-Za-z0-9._-]+", "_", str(value or "").strip())
    text = re.sub(r"_+", "_", text).strip("._-")
    return text or fallback


def _default_nodecontrol_artifact_dir(*, bind: str, node_id: str) -> str:
    _host, port = split_host_port(str(bind or ""))
    node_part = _safe_artifact_dir_part(node_id, fallback="node")
    return str((Path.cwd() / "code_cache" / f"{node_part}-{int(port)}").resolve())


def _normalize_role(value: str) -> str:
    text = str(value or "").strip().lower()
    if text.startswith("info"):
        return "infocenter"
    if text.startswith("gate"):
        return "gateway"
    if text.startswith("job"):
        return "joborchestrator"
    if text.startswith("node"):
        return "nodecontrol"
    if text.startswith("cont"):
        return "controlplane"
    raise argparse.ArgumentTypeError(
        "role must start with one of: info, gate, job, node"
    )


def _resolve_bind(bind: str, *, remote_hint: str = "") -> str:
    host, port = split_host_port(bind)
    return format_host_port(resolve_public_host(host, remote_hint=remote_hint), port)


def build_infocenter_server(bind: str, *, max_workers: int = 32) -> InfoCenterHttpServer:
    if int(max_workers or 0) != 32:
        logger.warning("InfoCenterHttpServer does not support max_workers; ignoring %s", max_workers)
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
    job_queue = JobQueueManager()
    route_cache = GatewayRouteCache(
        source=InProcessInfoCenterSource(info_state),
        refresh_interval_sec=gateway_refresh_interval_sec,
        failure_threshold=gateway_failure_threshold,
        open_sec=gateway_open_sec,
    )
    def _register_data_ref(**kwargs):
        from pycloud_parallel.controlplane.node_control_client import NodeControlClient

        entry = info_state.register_data_ref_record(**kwargs)
        pin_targets = list(entry.replicas or ())
        if not pin_targets and str(entry.control_addr or "").strip():
            pin_targets = [{"control_addr": str(entry.control_addr or "").strip()}]
        for item in pin_targets:
            control_addr = str(item.get("control_addr", "") or "").strip()
            if not control_addr:
                continue
            try:
                with NodeControlClient(control_addr, timeout_sec=0.5) as client:
                    client.pin_object(
                        object_id=str(entry.storage_id or entry.ref_id or ""),
                        ref_id=str(entry.ref_id or ""),
                    )
            except Exception:
                continue
        return entry
    gateway_app = GatewayHttpApp(
        route_cache=route_cache,
        register_data_ref=_register_data_ref,
        controlplane_target="",
    )
    return InfoCenterHttpServer(
        bind=bind,
        state=info_state,
        gateway_app=gateway_app,
        job_queue=job_queue,
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
    allow_private = str(os.getenv("PYCLOUD_GATEWAY_ALLOW_PRIVATE_ADDRS", "true") or "true").lower() in {"1", "true", "yes"}
    return GatewayHttpServer(
        bind=bind,
        app=GatewayHttpApp(
            route_cache=route_cache,
            allow_private_addrs=allow_private,
            controlplane_target=infocenter_addr,
        ),
    )


def build_job_orchestrator_server(
    bind: str,
    *,
    infocenter_addr: str,
    node_id: str,
    service_name: str = DEFAULT_JOB_ORCHESTRATOR_SERVICE_NAME,
    queue_capacity: int = NODE_QUEUE_CAPACITY,
    tags: Optional[list[str]] = None,
    version: str = "",
    taskpool_policy_id: str = "",
    admin_token: str = "",
) -> JobOrchestratorServer:
    if not infocenter_addr:
        raise ValueError("infocenter_addr is required for joborchestrator role")
    return JobOrchestratorServer(
        bind=bind,
        infocenter_addr=infocenter_addr,
        node_id=node_id,
        service_name=service_name,
        queue_capacity=queue_capacity,
        tags=tags,
        version=version,
        taskpool_policy_id=taskpool_policy_id,
        admin_token=admin_token,
    )


def build_nodecontrol_server(
    bind: str,
    *,
    node_id: str,
    artifact_dir: str = "",
    worker_capacity: int = NODE_WORKER_CAPACITY,
    queue_capacity: int = NODE_QUEUE_CAPACITY,
    max_workers: int = NODE_MAX_WORKERS,
    service_http_bind: str = "0.0.0.0:18080",
    service_http_base_url: str = "",
    node_http_base_url: str = "",
    executor_backend: str = EXECUTOR_BACKEND,
    service_default_worker_count: int = SERVICE_DEFAULT_WORKERS,
    service_default_heartbeat_timeout_sec: int = SERVICE_HEARTBEAT_TIMEOUT_SEC,
    on_service_routes_changed: Optional[Callable[[], None]] = None,
) -> Tuple[NodeControlHttpServer, NodeControlState]:
    del max_workers
    state = NodeControlState(
        node_id=node_id,
        artifact_dir=artifact_dir or _default_nodecontrol_artifact_dir(bind=bind, node_id=node_id),
        worker_capacity=worker_capacity,
        queue_capacity=queue_capacity,
        service_http_bind=service_http_bind,
        service_http_base_url=service_http_base_url,
        node_http_base_url=node_http_base_url,
        executor_backend=executor_backend,
        service_default_worker_count=service_default_worker_count,
        service_default_heartbeat_timeout_sec=service_default_heartbeat_timeout_sec,
    )
    server = NodeControlHttpServer(
        bind=bind,
        state=state,
        on_service_routes_changed=on_service_routes_changed,
    )
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
        server: control-plane server
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
        server.stop()

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
            logger.warning("failed to register signal handler %s (not in main thread or unsupported)", sig_name)
            continue
    server.wait_for_termination()


def main() -> None:
    """主函数。

    根据命令行参数启动 InfoCenter 或 NodeControl 服务器。
    """
    parser = argparse.ArgumentParser(description="PyCloud control-plane server")
    parser.add_argument("--role", type=_normalize_role, required=True)
    parser.add_argument("--bind", default="")
    parser.add_argument("--node-id", default="node-local-01")
    parser.add_argument("--artifact-dir", default="")
    parser.add_argument("--service-name", default=DEFAULT_JOB_ORCHESTRATOR_SERVICE_NAME)
    parser.add_argument("--queue-capacity", type=int, default=NODE_QUEUE_CAPACITY)
    parser.add_argument("--worker-capacity", type=int, default=NODE_WORKER_CAPACITY)
    parser.add_argument("--max-workers", type=int, default=NODE_MAX_WORKERS)
    parser.add_argument("--service-http-bind", default="")
    parser.add_argument("--service-http-base-url", default="")
    parser.add_argument("--node-http-bind", default="", help="HTTP NodeControl bind address; defaults to --bind for nodecontrol")
    parser.add_argument("--node-http-base-url", default="", help="public base URL for HTTP NodeControl; derived from --node-http-bind when omitted")
    parser.add_argument(
        "--executor-backend",
        default=EXECUTOR_BACKEND,
        choices=("subprocess_host",),
    )
    parser.add_argument("--service-default-workers", type=int, default=SERVICE_DEFAULT_WORKERS)
    parser.add_argument("--service-heartbeat-timeout-sec", type=int, default=SERVICE_HEARTBEAT_TIMEOUT_SEC)
    parser.add_argument("--infocenter-addr", default="")
    parser.add_argument("--advertise-addr", default="")
    parser.add_argument("--node-tags", default="compute")
    parser.add_argument("--node-version", default="v1")
    parser.add_argument("--taskpool-policy-id", default="")
    parser.add_argument("--gateway-refresh-interval-sec", type=float, default=3.0)
    parser.add_argument("--gateway-failure-threshold", type=int, default=3)
    parser.add_argument("--gateway-open-sec", type=float, default=5.0)
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()
    if not str(args.bind or "").strip():
        default_port = (
            50051
            if args.role in ("infocenter", "controlplane")
            else (50052 if args.role == "gateway" else (50053 if args.role == "joborchestrator" else 18061))
        )
        args.bind = format_host_port(detect_local_ip(remote_hint=str(args.infocenter_addr or "")), default_port)
    if args.role == "nodecontrol" and not str(args.service_http_bind or "").strip():
        args.service_http_bind = format_host_port(detect_local_ip(remote_hint=str(args.infocenter_addr or "")), 18080)

    level_name = str(args.log_level or "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    if args.role == "infocenter":
        bind = _resolve_bind(args.bind)
        logger.info("[Server] starting InfoCenter bind=%s log_level=%s", bind, level_name)
        server = build_infocenter_server(bind, max_workers=args.max_workers)
        _wait_until_stopped(server, on_stop=lambda: None)
        return

    if args.role == "controlplane":
        bind = _resolve_bind(args.bind)
        logger.info("[Server] starting ControlPlane bind=%s log_level=%s", bind, level_name)
        server = build_controlplane_server(
            bind,
            gateway_refresh_interval_sec=args.gateway_refresh_interval_sec,
            gateway_failure_threshold=args.gateway_failure_threshold,
            gateway_open_sec=args.gateway_open_sec,
        )
        _wait_until_stopped(server, on_stop=lambda: None)
        return

    if args.role == "gateway":
        bind = _resolve_bind(args.bind, remote_hint=args.infocenter_addr)
        logger.info(
            "[Server] starting Gateway bind=%s infocenter=%s log_level=%s",
            bind,
            args.infocenter_addr,
            level_name,
        )
        server = build_gateway_server(
            bind,
            infocenter_addr=args.infocenter_addr,
            gateway_refresh_interval_sec=args.gateway_refresh_interval_sec,
            gateway_failure_threshold=args.gateway_failure_threshold,
            gateway_open_sec=args.gateway_open_sec,
        )
        _wait_until_stopped(server, on_stop=lambda: None)
        return

    if args.role == "joborchestrator":
        bind = _resolve_bind(args.bind, remote_hint=args.infocenter_addr)
        orchestrator_node_id = str(args.node_id or "").strip() or "job-orchestrator-01"
        if orchestrator_node_id == "node-local-01":
            orchestrator_node_id = "job-orchestrator-01"
        logger.info(
            "[Server] starting JobOrchestrator bind=%s infocenter=%s service_name=%s log_level=%s",
            bind,
            args.infocenter_addr,
            args.service_name,
            level_name,
        )
        server = build_job_orchestrator_server(
            bind,
            infocenter_addr=args.infocenter_addr,
            node_id=orchestrator_node_id,
            service_name=args.service_name,
            queue_capacity=args.queue_capacity,
            tags=[x.strip() for x in args.node_tags.split(",") if x.strip()] or ["job"],
            version=args.node_version,
            taskpool_policy_id=args.taskpool_policy_id,
            admin_token=str(getattr(args, "admin_token", "") or ""),
        )
        _wait_until_stopped(server, on_stop=lambda: None)
        return

    bind = _resolve_bind(args.bind, remote_hint=args.infocenter_addr)
    service_http_bind = _resolve_bind(args.service_http_bind, remote_hint=args.infocenter_addr)
    advertise_addr = str(args.advertise_addr or "").strip()
    service_http_base_url = str(args.service_http_base_url or "").strip()
    if not service_http_base_url:
        advertise_host, _advertise_port = split_host_port(bind)
        _service_host, service_port = split_host_port(service_http_bind)
        service_http_base_url = f"http://{resolve_public_host(advertise_host, remote_hint=args.infocenter_addr)}:{int(service_port)}"
    node_http_bind = str(args.node_http_bind or "").strip() or bind
    node_http_base_url = str(args.node_http_base_url or "").strip()
    node_http_bind = _resolve_bind(node_http_bind, remote_hint=args.infocenter_addr)
    if not node_http_base_url:
        node_http_host, node_http_port = split_host_port(node_http_bind)
        node_http_base_url = f"http://{resolve_public_host(node_http_host, remote_hint=args.infocenter_addr)}:{int(node_http_port)}"
    if not advertise_addr:
        advertise_addr = node_http_base_url
    logger.info(
        "[Server] starting NodeControl HTTP bind=%s node_id=%s infocenter=%s advertise=%s log_level=%s",
        node_http_bind,
        args.node_id,
        args.infocenter_addr,
        advertise_addr,
        level_name,
    )
    node_tags = [x.strip() for x in args.node_tags.split(",") if x.strip()]

    registrar_holder: dict[str, Optional[NodeInfoCenterRegistrar]] = {"value": None}

    def _sync_routes_now() -> None:
        registrar = registrar_holder["value"]
        if registrar is not None:
            registrar.request_sync()

    server, state = build_nodecontrol_server(
        node_http_bind,
        node_id=args.node_id,
        artifact_dir=str(args.artifact_dir or "").strip(),
        queue_capacity=args.queue_capacity,
        worker_capacity=args.worker_capacity,
        max_workers=args.max_workers,
        service_http_bind=service_http_bind,
        service_http_base_url=service_http_base_url,
        node_http_base_url=node_http_base_url,
        executor_backend=args.executor_backend,
        service_default_worker_count=args.service_default_workers,
        service_default_heartbeat_timeout_sec=args.service_heartbeat_timeout_sec,
        on_service_routes_changed=_sync_routes_now,
    )
    node_http_server: Optional[NodeControlHttpServer] = server

    registrar: Optional[NodeInfoCenterRegistrar] = None
    if args.infocenter_addr:
        registrar = NodeInfoCenterRegistrar(
            infocenter_addr=args.infocenter_addr,
            node_id=args.node_id,
            control_addr=node_http_base_url,
            state=state,
            capacity=args.worker_capacity,
            queue_capacity=args.queue_capacity,
            tags=node_tags,
            version=args.node_version,
            metadata={"role": "compute-node"},
        )
        registrar_holder["value"] = registrar

    def _on_start() -> None:
        logger.info("[Server] HTTP NodeControl started base_url=%s", node_http_server.base_url)
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
        logger.info("[Server] HTTP NodeControl stop base_url=%s", node_http_server.base_url)
        state.close()

    _wait_until_stopped(server, on_stop=_on_stop, on_start=_on_start)


if __name__ == "__main__":
    main()
