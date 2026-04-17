#!/usr/bin/env python3
"""直接启动一个 ControlPlane（InfoCenter + Gateway）。

默认只需要：

python examples/start_controlplane.py

也可以覆盖默认参数，例如：

python examples/start_controlplane.py \
  --bind 0.0.0.0:50051 \
  --gateway-refresh-interval-sec 2.0 \
  --log-level DEBUG
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

from pycloud_parallel.controlplane.server import build_controlplane_server

DEFAULT_CONFIG = {
    "bind": "0.0.0.0:50051",
    "gateway_refresh_interval_sec": 3.0,
    "gateway_failure_threshold": 3,
    "gateway_open_sec": 5.0,
    "log_level": "INFO",
}


def main() -> None:
    parser = argparse.ArgumentParser(description="Start a single PyCloud ControlPlane")
    parser.add_argument("--bind", default=DEFAULT_CONFIG["bind"], help="controlplane HTTP bind address")
    parser.add_argument(
        "--gateway-refresh-interval-sec",
        type=float,
        default=DEFAULT_CONFIG["gateway_refresh_interval_sec"],
    )
    parser.add_argument(
        "--gateway-failure-threshold",
        type=int,
        default=DEFAULT_CONFIG["gateway_failure_threshold"],
    )
    parser.add_argument(
        "--gateway-open-sec",
        type=float,
        default=DEFAULT_CONFIG["gateway_open_sec"],
    )
    parser.add_argument("--log-level", default=DEFAULT_CONFIG["log_level"])
    args = parser.parse_args()

    level_name = str(args.log_level or "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    logger = logging.getLogger(__name__)

    server = build_controlplane_server(
        args.bind,
        gateway_refresh_interval_sec=float(args.gateway_refresh_interval_sec),
        gateway_failure_threshold=int(args.gateway_failure_threshold),
        gateway_open_sec=float(args.gateway_open_sec),
    )

    stop_called = False

    def _graceful_stop(*_args) -> None:
        nonlocal stop_called
        if stop_called:
            return
        stop_called = True
        logger.info("stopping controlplane bind=%s", args.bind)
        try:
            server.stop()
        except Exception:
            pass

    for sig_name in ("SIGINT", "SIGTERM"):
        sig = getattr(signal, sig_name, None)
        if sig is None:
            continue
        try:
            signal.signal(sig, _graceful_stop)
        except (ValueError, OSError):
            continue

    logger.info(
        "starting controlplane bind=%s gateway_refresh_interval_sec=%s gateway_failure_threshold=%s gateway_open_sec=%s",
        args.bind,
        args.gateway_refresh_interval_sec,
        args.gateway_failure_threshold,
        args.gateway_open_sec,
    )
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    main()
