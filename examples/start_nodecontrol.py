#!/usr/bin/env python3
"""直接启动一个 NodeControl 节点。

示例：

python examples/start_nodecontrol.py \
  --bind 0.0.0.0:50062 \
  --node-id node-2 \
  --infocenter-addr 127.0.0.1:50051 \
  --advertise-addr 127.0.0.1:50062 \
  --service-http-bind 127.0.0.1:18082
"""

from __future__ import annotations

from pathlib import Path
import sys

REPO_SRC = Path(__file__).resolve().parents[1] / "src"
if str(REPO_SRC) not in sys.path:
    sys.path.insert(0, str(REPO_SRC))

import argparse
import logging
import signal
from typing import Optional

from pycloud_parallel.controlplane.config import (
    NODE_MAX_WORKERS,
    NODE_QUEUE_CAPACITY,
    NODE_WORKER_CAPACITY,
    SERVICE_DEFAULT_WORKERS,
    SERVICE_HEARTBEAT_TIMEOUT_SEC,
)
from pycloud_parallel.controlplane.registrar import NodeInfoCenterRegistrar
from pycloud_parallel.controlplane.server import build_nodecontrol_server

DEFAULT_CONFIG = {
    "bind": "0.0.0.0:50061",
    "node_id": "node-local-01",
    "infocenter_addr": "127.0.0.1:50051",
    "advertise_addr": "",
    "service_http_bind": "127.0.0.1:18081",
    "service_http_base_url": "",
    "worker_capacity": NODE_WORKER_CAPACITY,
    "queue_capacity": NODE_QUEUE_CAPACITY,
    "max_workers": NODE_MAX_WORKERS,
    "service_default_workers": SERVICE_DEFAULT_WORKERS,
    "service_heartbeat_timeout_sec": SERVICE_HEARTBEAT_TIMEOUT_SEC,
    "node_tags": "compute",
    "node_version": "v1",
    "log_level": "INFO",
}


def main() -> None:
    parser = argparse.ArgumentParser(description="Start a single PyCloud NodeControl node")
    parser.add_argument("--bind", default=DEFAULT_CONFIG["bind"], help="NodeControl HTTP bind address")
    parser.add_argument("--node-id", default=DEFAULT_CONFIG["node_id"], help="logical node id")
    parser.add_argument("--infocenter-addr", default=DEFAULT_CONFIG["infocenter_addr"], help="InfoCenter address")
    parser.add_argument("--advertise-addr", default=DEFAULT_CONFIG["advertise_addr"], help="address advertised to InfoCenter")
    parser.add_argument("--service-http-bind", default=DEFAULT_CONFIG["service_http_bind"], help="service HTTP bind address")
    parser.add_argument("--service-http-base-url", default=DEFAULT_CONFIG["service_http_base_url"], help="external service HTTP base url")
    parser.add_argument("--worker-capacity", type=int, default=DEFAULT_CONFIG["worker_capacity"])
    parser.add_argument("--queue-capacity", type=int, default=DEFAULT_CONFIG["queue_capacity"])
    parser.add_argument("--max-workers", type=int, default=DEFAULT_CONFIG["max_workers"])
    parser.add_argument("--service-default-workers", type=int, default=DEFAULT_CONFIG["service_default_workers"])
    parser.add_argument("--service-heartbeat-timeout-sec", type=int, default=DEFAULT_CONFIG["service_heartbeat_timeout_sec"])
    parser.add_argument("--node-tags", default=DEFAULT_CONFIG["node_tags"], help="comma separated tags")
    parser.add_argument("--node-version", default=DEFAULT_CONFIG["node_version"])
    parser.add_argument("--log-level", default=DEFAULT_CONFIG["log_level"])
    args = parser.parse_args()

    level_name = str(args.log_level or "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    logger = logging.getLogger(__name__)

    advertise_addr = (args.advertise_addr or args.bind).strip()
    node_tags = [x.strip() for x in str(args.node_tags or "").split(",") if x.strip()]

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

    stop_called = False

    def _graceful_stop(*_args) -> None:
        nonlocal stop_called
        if stop_called:
            return
        stop_called = True
        logger.info("stopping nodecontrol node_id=%s", args.node_id)
        if registrar is not None:
            registrar.close()
        state.close()
        server.stop()

    for sig_name in ("SIGINT", "SIGTERM"):
        sig = getattr(signal, sig_name, None)
        if sig is None:
            continue
        try:
            signal.signal(sig, _graceful_stop)
        except (ValueError, OSError):
            continue

    logger.info(
        "starting nodecontrol bind=%s node_id=%s infocenter=%s advertise=%s service_http_bind=%s",
        args.bind,
        args.node_id,
        args.infocenter_addr,
        advertise_addr,
        args.service_http_bind,
    )
    server.start()
    if registrar is not None:
        registrar.start()
    server.wait_for_termination()


if __name__ == "__main__":
    main()
