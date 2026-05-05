#!/usr/bin/env python3
"""
PyCloud Gateway 完整演示

演示完整的流程：
1. 部署一个服务（使用 Service）
2. 通过 `Service.connect(..., route="gateway")` 按服务名调用
3. 清理服务
"""

from pathlib import Path
import sys

REPO_SRC = Path(__file__).resolve().parents[1] / "src"
if str(REPO_SRC) not in sys.path:
    sys.path.insert(0, str(REPO_SRC))

import asyncio
import time
from pycloud_parallel import (
    Service,
)


def main():
    gateway_target = "127.0.0.1:50051"
    service_name = "square-service"

    print("=" * 60)
    print("  PyCloud Gateway 完整演示")
    print("=" * 60)
    print(f"  Gateway: {gateway_target}")
    print(f"  Service: {service_name}")
    print()

    # 步骤 1: 部署服务
    print("[1] 部署服务...")
    print("-" * 60)

    blob = (
        b"from pycloud_parallel import export\n\n"
        b"@export\n"
        b"def square(x):\n"
        b"    return {'x': x, 'y': x * x}\n"
        b"\n"
        b"@export\n"
        b"def cube(x):\n"
        b"    return {'x': x, 'y': x * x * x}\n"
    )

    group = Service.deploy(
        target=gateway_target,
        service_name=service_name,
        source=blob,
        runtime="py3",
        worker_count=4,
        tags=["compute"],
        min_success_nodes=1,
    )
    print(f"✓ 服务部署成功")
    print(f"  服务名: {group.service_name}")
    print(f"  节点: {list(group.sessions.keys())}")
    print()

    # 启动心跳
    time.sleep(5)  # 等待服务启动

    try:
        # 步骤 2: 使用 Service.connect(gateway) 调用
        print("[2] 使用 Service.connect(gateway)")
        print("-" * 60)

        with Service.connect(
            target=gateway_target,
            service_name=service_name,
            route="gateway",
            timeout_sec=10.0,
        ) as client:
            # 列出方法
            methods = client.methods
            print("可用方法:")
            for method_name in methods:
                print(f"  - {method_name}")
            print()

            # 获取状态
            status = client.status()
            print(f"路由数量: {status.get('route_count')}")
            for route in status.get("routes", []):
                print(
                    f"  - node={route.get('node_id')} "
                    f"service_id={route.get('service_id')[:8]}... "
                    f"in_flight={route.get('in_flight')}"
                )
            print()

            # 调用服务
            print("调用服务:")
            result1 = client.square.sync(x=7)
            print(f"  square(7) = {result1}")

            result2 = client.cube.sync(x=3)
            print(f"  cube(3) = {result2}")
        print()

        # 步骤 3: 继续使用 Service.connect(gateway) 调用
        print("[3] 继续使用 Service.connect(gateway)")
        print("-" * 60)
        with Service.connect(
            target=gateway_target,
            service_name=service_name,
            route="gateway",
            timeout_sec=10.0,
        ) as client:
            print(f"可用方法: {client.methods}")
        print()

        # 同步调用
        print("同步调用:")
        result3 = client.square.sync(x=9)
        print(f"  square(9) = {result3}")

        result4 = client.cube.sync(x=2)
        print(f"  cube(2) = {result4}")
        print()

        # 异步调用
        print("异步调用:")

        async def async_calls():
            result5 = await client.square(x=11)
            print(f"  square(11) = {result5}")

            result6 = await client.cube(x=4)
            print(f"  cube(4) = {result6}")

            # 并发调用
            results = await asyncio.gather(*[client.square(x=value) for value in (1, 2, 3)])
            print(f"  并发调用 square(1,2,3) = {results}")

        asyncio.run(async_calls())
        print()

    finally:
        # 步骤 4: 清理服务
        print("[4] 清理服务")
        print("-" * 60)
        group.close(end_services=True)
        print("✓ 服务已停止")
        print()

    print("=" * 60)
    print("  完成")
    print("=" * 60)


if __name__ == "__main__":
    main()
