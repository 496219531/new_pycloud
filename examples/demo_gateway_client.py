#!/usr/bin/env python3
"""
PyCloud Gateway 调用示例

演示如何通过 controlplane 的 Gateway 按 service_name 调用服务。
同一个脚本里同时展示：
1. GatewayServiceClient：薄 HTTP helper
2. GatewayConnect：module-like caller

前置条件：
- 需要先部署名为 "square-service" 的服务
- 可以运行 demo_gateway_complete.py 来自动部署和演示

或者手动部署：
```python
from pycloud_parallel import DeployedService, pycloud_export

blob = (
    b"from pycloud_parallel import pycloud_export\n\n"
    b"@pycloud_export\n"
    b"def square(x=0, **_kwargs):\n"
    b"    x = int(x)\n"
    b"    return {'x': x, 'y': x * x}\n"
)

group = DeployedService.deploy_from_infocenter(
    infocenter_target="127.0.0.1:50051",
    service_name="square-service",
    blob=blob,
    entry_module="square_service",
    dependency_allowlist=["./third_party/my_local_pkg"],  # 可选
)
```
"""

import asyncio
from pycloud_parallel import GatewayConnect
from pycloud_parallel.controlplane.client import GatewayServiceClient

def check_service_exists(gateway_target: str, service_name: str) -> bool:
    """检查服务是否存在。"""
    with GatewayServiceClient(gateway_target, timeout_sec=5.0) as client:
        status = client.get_status(service_name=service_name)
        return status.get("route_count", 0) > 0


def main() -> None:
    gateway_target = "127.0.0.1:50051"
    service_name = "compute-service"

    print("=" * 60)
    print("  PyCloud Gateway Client Demo")
    print("=" * 60)
    print()
    print(f"  Gateway: {gateway_target}")
    print(f"  Service: {service_name}")
    print()

    # 检查服务是否存在
    if not check_service_exists(gateway_target, service_name):
        raise RuntimeError(
            f"service {service_name!r} not found; deploy it first with "
            "python examples/demo_gateway_complete.py"
        )

    print("[GatewayServiceClient]")
    print("-" * 60)

    with GatewayServiceClient(gateway_target, timeout_sec=10.0) as client:
        methods = client.list_methods(service_name=service_name, include_docs=False)
        print("可用方法:")
        for item in methods:
            print(f"  - {item.get('method')}")
        print()

        status = client.get_status(service_name=service_name)
        print(f"路由数量: {status.get('route_count')}")
        for route in status.get("routes", []):
            print(
                "  - "
                f"node={route.get('node_id')} "
                f"service_id={route.get('service_id')[:8]}... "
                f"in_flight={route.get('in_flight')}"
            )
        print()

        print("调用服务:")
        resp = client.call(
            service_name=service_name,
            method="square",
            payload={"x": 7},
            timeout_sec=10.0,
        )
        print(f"  square(7) = {resp}")

    print()
    print("[GatewayConnect]")
    print("-" * 60)
    module_client = GatewayConnect(
        gateway_target,
        service_name=service_name,
        timeout_sec=10.0,
    )
    print(f"可用方法: {module_client.methods}")
    print()

    print("同步调用:")
    print(f"  square(9) = {module_client.square.sync(x=9)}")
    print()

    print("异步调用:")

    async def _run() -> None:
        result = await module_client.square(x=11)
        print(f"  square(11) = {result}")

    asyncio.run(_run())


if __name__ == "__main__":
    main()
