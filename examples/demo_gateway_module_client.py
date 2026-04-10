#!/usr/bin/env python3
"""
PyCloud Gateway module-like caller 示例。

演示如何像本地模块一样，通过 controlplane Gateway 调用远程服务。
"""

import asyncio
import time

from pycloud_parallel import DeployedService, GatewayConnect


def _service_exists(target: str, service_name: str) -> bool:
    from pycloud_parallel.controlplane.client import GatewayServiceClient

    try:
        with GatewayServiceClient(target, timeout_sec=5.0) as client:
            status = client.get_status(service_name=service_name)
            return status.get("route_count", 0) > 0
    except RuntimeError:
        return False


def _ensure_service(target: str, service_name: str):
    blob = (
        b"from pycloud_parallel import pycloud_export\n\n"
        b"@pycloud_export\n"
        b"def square(x=0, **_kwargs):\n"
        b"    x = int(x)\n"
        b"    return {'x': x, 'y': x * x}\n"
    )
    group = DeployedService.deploy_from_infocenter(
        infocenter_target=target,
        owner_client_id=f"gateway-module-demo-{int(time.time())}",
        service_name=service_name,
        blob=blob,
        runtime="py3",
        entry_module="square_service",
        export_mode="decorator",
        worker_count=1,
        tags=["compute"],
        min_success_nodes=1,
    )
    deadline = time.time() + 5.0
    while time.time() < deadline:
        if _service_exists(target, service_name):
            break
        time.sleep(0.2)
    return group


def main() -> None:
    service_name = f"square-service-{int(time.time())}"
    group = _ensure_service("127.0.0.1:50051", service_name)
    client = GatewayConnect(
        "127.0.0.1:50051",
        service_name=service_name,
        timeout_sec=10.0,
    )

    try:
        print("=" * 60)
        print("  PyCloud Gateway Module Client Demo")
        print("=" * 60)
        print()
        print(f"  Service: {client.service_name}")
        print(f"  Methods: {client.methods}")
        print()

        print("[+] sync call")
        print(f"    square.sync(x=7) -> {client.square.sync(x=7)}")
        print()

        async def _run() -> None:
            print("[+] async call")
            print(f"    await square(x=11) -> {await client.square(x=11)}")
            print()
            print("[+] generic async call")
            print(f"    await call('square', x=13) -> {await client.call('square', x=13)}")

        asyncio.run(_run())
    finally:
        if group is not None:
            group.close(end_services=True, reason="demo_gateway_module_client cleanup")


if __name__ == "__main__":
    main()
