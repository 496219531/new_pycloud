#!/usr/bin/env python3
"""
PyCloud Gateway 调用示例

演示如何通过 controlplane 的 Gateway 按 service_name 调用服务。
同一个脚本里同时展示：
1. GatewayServiceClient：薄 HTTP helper
2. GatewayModuleClient：module-like caller
"""

import asyncio

from pycloud_parallel.controlplane.client import GatewayModuleClient, GatewayServiceClient


def main() -> None:
    gateway_target = "127.0.0.1:50051"
    service_name = "square-service"

    print("=" * 60)
    print("  PyCloud Gateway Client Demo")
    print("=" * 60)
    print()
    print(f"  Gateway: {gateway_target}")
    print(f"  Service: {service_name}")
    print()

    with GatewayServiceClient(gateway_target, timeout_sec=10.0) as client:
        print("[GatewayServiceClient]")
        methods = client.list_methods(service_name=service_name, include_docs=False)
        print("[+] Methods:")
        for item in methods:
            print(f"    - {item.get('method')}")
        print()

        status = client.get_status(service_name=service_name)
        print(f"[+] Route count: {status.get('route_count')}")
        for route in status.get("routes", []):
            print(
                "    - "
                f"node={route.get('node_id')} "
                f"service_id={route.get('service_id')} "
                f"in_flight={route.get('in_flight')}"
            )
        print()

        resp = client.call(
            service_name=service_name,
            method="square",
            payload={"x": 7},
            timeout_sec=10.0,
        )
        print("[+] Call result:")
        print(f"    {resp}")

    print()
    print("[GatewayModuleClient]")
    module_client = GatewayModuleClient(
        gateway_target,
        service_name=service_name,
        timeout_sec=10.0,
    )
    print(f"[+] Methods: {module_client.methods}")
    print(f"[+] sync call: {module_client.square.sync(x=9)}")

    async def _run() -> None:
        result = await module_client.square(x=11)
        print(f"[+] async call: {result}")

    asyncio.run(_run())


if __name__ == "__main__":
    main()
