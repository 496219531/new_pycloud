#!/usr/bin/env python3
"""
通过 `Service.connect(..., transport="gateway")` 调用已部署服务。

这个示例聚焦 gateway 连接主路径：
1. `Service.connect(..., transport="gateway")`
2. 统一服务对象的 `methods / status / foo.sync(...)`

前置条件：
- 需要先部署名为 "square-service" 的服务
- 可以运行 `service_gateway_end_to_end.py` 自动完成部署和调用演示
"""
    b"@export\n"
    b"def square(x=0, **_kwargs):\n"
    b"    x = int(x)\n"
    b"    return {'x': x, 'y': x * x}\n"
)

group = Service.deploy(
    infocenter_target="127.0.0.1:50051",
    service_name="square-service",
    blob=blob,
    entry_module="square_service",
    dependency_allowlist=["./third_party/my_local_pkg"],  # 可选
)
```
"""

from pathlib import Path
import sys

REPO_SRC = Path(__file__).resolve().parents[1] / "src"
if str(REPO_SRC) not in sys.path:
    sys.path.insert(0, str(REPO_SRC))

import asyncio
import time
from pycloud_parallel import Service


def check_service_exists(gateway_target: str, service_name: str) -> bool:
    """检查服务是否存在。"""
    try:
        with Service.connect(
            target=gateway_target,
            service_name=service_name,
            transport="gateway",
            timeout_sec=5.0,
            validate_on_init=False,
        ) as client:
            status = client.status()
            return status.get("route_count", 0) > 0
    except RuntimeError:
        return False


def wait_for_service_ready(gateway_target: str, service_name: str, *, timeout_sec: float = 8.0) -> None:
    deadline = time.time() + max(1.0, float(timeout_sec))
    last_error = "service route not ready"
    while time.time() < deadline:
        try:
            with Service.connect(
                target=gateway_target,
                service_name=service_name,
                transport="gateway",
                timeout_sec=5.0,
                validate_on_init=False,
            ) as client:
                status = client.status()
                if status.get("route_count", 0) <= 0:
                    raise RuntimeError("route_count=0")
                client.square.sync(x=1)
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
    group = Service.deploy(
        infocenter_target=gateway_target,
        owner_client_id=f"gateway-client-demo-{int(time.time())}",
        service_name=service_name,
        source=blob,
        runtime="py3",
        entry_module="square_service",
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
        print("[Service.connect(gateway)]")
        print("-" * 60)

        with Service.connect(
            target=gateway_target,
            service_name=service_name,
            transport="gateway",
            timeout_sec=10.0,
        ) as client:
            methods = client.methods
            print("可用方法:")
            for method_name in methods:
                print(f"  - {method_name}")
            print()

            status = client.status()
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
            resp = client.square.sync(x=7)
            print(f"  square(7) = {resp}")

        print()
        print("[Service.connect(gateway) - method calls]")
        print("-" * 60)
        with Service.connect(
            target=gateway_target,
            service_name=service_name,
            transport="gateway",
            timeout_sec=10.0,
        ) as client:
            status = client.status()
            print(f"route_count={status.get('route_count')}")
            print("同步调用:")
            print(f"  square(9) = {client.square.sync(x=9)}")
            print()

            print("异步示例:")

            async def _run() -> None:
                result = await client.square(x=11)
                print(f"  square(11) = {result}")

            asyncio.run(_run())
    finally:
        if group is not None:
            group.close(end_services=True, reason="demo_gateway_client cleanup")


if __name__ == "__main__":
    main()
