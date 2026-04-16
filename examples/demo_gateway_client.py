#!/usr/bin/env python3
"""
PyCloud Gateway 调用示例

演示如何通过 controlplane 的 Gateway 按 service_name 调用服务。
同一个脚本里同时展示：
1. GatewayServiceClient：薄 HTTP helper
2. GatewayServiceClient：直接按方法名调用

前置条件：
- 需要先部署名为 "square-service" 的服务
- 可以运行 demo_gateway_complete.py 来自动部署和演示

或者手动部署：
```python
from pycloud_parallel import Service, export

blob = (
    b"from pycloud_parallel import export\n\n"
    b"@export\n"
    b"def square(x=0, **_kwargs):\n"
    b"    x = int(x)\n"
    b"    return {'x': x, 'y': x * x}\n"
)

group = Service.deploy_from_infocenter(
    infocenter_target="127.0.0.1:50051",
    service_name="square-service",
    blob=blob,
    entry_module="square_service",
    dependency_allowlist=["./third_party/my_local_pkg"],  # 可选
)
```
"""

import asyncio
import time
from pycloud_parallel import Service
from pycloud_parallel.controlplane.client import GatewayServiceClient


def check_service_exists(gateway_target: str, service_name: str) -> bool:
    """检查服务是否存在。"""
    try:
        with GatewayServiceClient(gateway_target, timeout_sec=5.0) as client:
            status = client.get_status(service_name=service_name)
            return status.get("route_count", 0) > 0
    except RuntimeError:
        return False


def wait_for_service_ready(gateway_target: str, service_name: str, *, timeout_sec: float = 8.0) -> None:
    deadline = time.time() + max(1.0, float(timeout_sec))
    last_error = "service route not ready"
    while time.time() < deadline:
        try:
            with GatewayServiceClient(gateway_target, timeout_sec=5.0) as client:
                status = client.get_status(service_name=service_name)
                if status.get("route_count", 0) <= 0:
                    raise RuntimeError("route_count=0")
                client.call(
                    service_name=service_name,
                    method="square",
                    payload={"x": 1},
                    timeout_sec=5.0,
                )
            return
        except RuntimeError as exc:
            last_error = str(exc)
            time.sleep(0.2)
    raise RuntimeError(f"service {service_name} not ready via gateway within {timeout_sec:.1f}s: {last_error}")


def ensure_service(gateway_target: str, service_name: str):
    if check_service_exists(gateway_target, service_name):
        wait_for_service_ready(gateway_target, service_name)
        return None
    blob = (
        b"from pycloud_parallel import export\n\n"
        b"@export\n"
        b"def square(x=0, **_kwargs):\n"
        b"    x = int(x)\n"
        b"    return {'x': x, 'y': x * x}\n"
    )
    group = Service.deploy_from_infocenter(
        infocenter_target=gateway_target,
        owner_client_id=f"gateway-client-demo-{int(time.time())}",
        service_name=service_name,
        blob=blob,
        runtime="py3",
        entry_module="square_service",
        export_mode="decorator",
        worker_count=1,
        tags=["compute"],
        min_success_nodes=1,
    )
    wait_for_service_ready(gateway_target, service_name)
    return group


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

    group = ensure_service(gateway_target, service_name)

    try:
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
        print("[GatewayServiceClient - method calls]")
        print("-" * 60)
        with GatewayServiceClient(gateway_target, timeout_sec=10.0) as client:
            status = client.get_status(service_name=service_name)
            print(f"route_count={status.get('route_count')}")
            print("同步调用:")
            print(
                "  square(9) = "
                f"{client.call(service_name=service_name, method='square', payload={'x': 9}, timeout_sec=10.0)}"
            )
            print()

            print("异步示例:")

            async def _run() -> None:
                loop = asyncio.get_running_loop()
                result = await loop.run_in_executor(
                    None,
                    lambda: client.call(
                        service_name=service_name,
                        method="square",
                        payload={"x": 11},
                        timeout_sec=10.0,
                    ),
                )
                print(f"  square(11) = {result}")

            asyncio.run(_run())
    finally:
        if group is not None:
            group.close(end_services=True, reason="demo_gateway_client cleanup")


if __name__ == "__main__":
    main()
