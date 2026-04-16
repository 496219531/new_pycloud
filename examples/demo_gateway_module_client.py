#!/usr/bin/env python3
"""
PyCloud Gateway caller 示例。

演示如何像本地模块一样，通过 controlplane Gateway 调用远程服务。
"""

import asyncio
import time

from pycloud_parallel import Service


def _service_exists(target: str, service_name: str) -> bool:
    from pycloud_parallel.controlplane.gateway_client import GatewayServiceClient

    try:
        with GatewayServiceClient(target, timeout_sec=5.0) as client:
            status = client.get_status(service_name=service_name)
            return status.get("route_count", 0) > 0
    except RuntimeError:
        return False


def _ensure_service(target: str, service_name: str):
    blob = (
        b"from pycloud_parallel import export\n\n"
        b"@export\n"
        b"def square(x=0, **_kwargs):\n"
        b"    x = int(x)\n"
        b"    return {'x': x, 'y': x * x}\n"
    )
    group = Service.deploy_from_infocenter(
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
    from pycloud_parallel.controlplane.gateway_client import GatewayServiceClient

    client = GatewayServiceClient("127.0.0.1:50051", timeout_sec=10.0)

    try:
        print("=" * 60)
        print("  PyCloud Gateway Client Demo")
        print("=" * 60)
        print()
        print(f"  Service: {service_name}")
        methods = client.list_methods(service_name=service_name, include_docs=False)
        print(f"  Methods: {[item['method'] for item in methods]}")
        print()

        print("[+] sync call")
        print(
            "    square(x=7) -> "
            f"{client.call(service_name=service_name, method='square', payload={'x': 7}, timeout_sec=10.0)}"
        )
        print()

        async def _run() -> None:
            print("[+] async call")
            loop = asyncio.get_running_loop()
            print(
                "    await square(x=11) -> "
                f"{await loop.run_in_executor(None, lambda: client.call(service_name=service_name, method='square', payload={'x': 11}, timeout_sec=10.0))}"
            )

        asyncio.run(_run())
    finally:
        if group is not None:
            group.close(end_services=True, reason="demo_gateway_module_client cleanup")


if __name__ == "__main__":
    main()
