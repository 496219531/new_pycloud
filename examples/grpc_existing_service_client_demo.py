#!/usr/bin/env python3
"""
调用已部署服务示例。

这个脚本演示“不通过 Gateway，而是像 Eureka client 一样”：
1. 先通过 InfoCenter 发现 service route
2. 客户端本地维护 route cache
3. 直接调用节点内部 `/svc/{service_id}/call/{method}`

脚本里同时展示：
1. `DiscoveryServiceClient`：薄封装
2. `DiscoveryCallerFacade`：module-like caller
"""

from __future__ import annotations

import asyncio
import time

from pycloud_parallel.controlplane.discovery_client import DiscoveryCallerFacade, DiscoveryServiceClient
from pycloud_parallel.controlplane.infocenter_client import InfoCenterClient


def _wait_for_service_name(
    *,
    infocenter_target: str,
    service_name: str = "",
    service_name_prefix: str = "",
    timeout_sec: float = 8.0,
    poll_interval_sec: float = 1.0,
) -> str:
    deadline = time.time() + max(0.5, float(timeout_sec))
    while True:
        with InfoCenterClient(infocenter_target, timeout_sec=5.0) as client:
            routes = list(
                client.list_service_routes(
                    service_name=service_name,
                    healthy_only=True,
                    limit=200,
                )
            )
        if service_name:
            if routes:
                return service_name
        else:
            matched = sorted({route.service_name for route in routes if route.service_name.startswith(service_name_prefix)})
            if matched:
                return matched[0]
        if time.time() >= deadline:
            return ""
        time.sleep(max(0.1, float(poll_interval_sec)))


def main() -> None:
    infocenter_target = "127.0.0.1:50051"
    service_name = ""
    service_name_prefix = "square-service"

    print("=" * 60)
    print("  PyCloud Discovery Client Demo")
    print("=" * 60)
    print()
    print(f"  InfoCenter: {infocenter_target}")

    active_service_name = _wait_for_service_name(
        infocenter_target=infocenter_target,
        service_name=service_name,
        service_name_prefix=service_name_prefix,
        timeout_sec=8.0,
    )
    if not active_service_name:
        print("[!] 未发现可用服务，请先部署一个服务")
        return

    print(f"  Service: {active_service_name}")
    print()

    with DiscoveryServiceClient(infocenter_target, timeout_sec=10.0) as client:
        print("[DiscoveryServiceClient]")
        methods = client.list_methods(service_name=active_service_name, include_docs=False)
        print(f"[+] Methods: {[item.get('method') for item in methods]}")

        status = client.get_status(service_name=active_service_name)
        print(f"[+] Route count: {status.get('route_count')}")
        for route in status.get("routes", []):
            print(
                "    - "
                f"node={route.get('node_id')} "
                f"service_id={route.get('service_id')} "
                f"in_flight={route.get('in_flight')}"
            )

        resp = client.call(
            service_name=active_service_name,
            method="square",
            payload={"x": 7},
            timeout_sec=10.0,
        )
        print(f"[+] sync call: {resp.get('data')}")

    print()
    print("[DiscoveryCallerFacade]")
    module_client = DiscoveryCallerFacade(
        infocenter_target,
        service_name=active_service_name,
        timeout_sec=10.0,
    )
    try:
        print(f"[+] Methods: {module_client.methods}")
        print(f"[+] sync call: {module_client.square.sync(x=9)}")

        async def _run() -> None:
            result = await module_client.square(x=11)
            print(f"[+] async call: {result}")

        asyncio.run(_run())
    finally:
        module_client.close()


if __name__ == "__main__":
    main()
