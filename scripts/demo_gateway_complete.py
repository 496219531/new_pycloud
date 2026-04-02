#!/usr/bin/env python3
"""
PyCloud Gateway 完整演示

演示完整的流程：
1. 部署一个服务（使用 DeployedService）
2. 通过 Gateway 按服务名调用
3. 清理服务
"""

import asyncio
import time
from pycloud_parallel.controlplane.client import GatewayServiceClient
from pycloud_parallel import (
    DeployedService,
    GatewayConnect,
)


def main():
    gateway_target = "127.0.0.1:50051"
    service_name = "square-service"
    # 如果服务依赖节点未预装的包，可显式填 dependency_allowlist。
    dependency_allowlist = []

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
        b"def pycloud_export(fn):\n"
        b"    fn.__pycloud_export__ = True\n"
        b"    return fn\n\n"
        b"@pycloud_export\n"
        b"def square(x):\n"
        b"    return {'x': x, 'y': x * x}\n"
        b"\n"
        b"@pycloud_export\n"
        b"def cube(x):\n"
        b"    return {'x': x, 'y': x * x * x}\n"
    )

    group = DeployedService.deploy_from_infocenter(
        infocenter_target=gateway_target,
        service_name=service_name,
        blob=blob,
        filename="square_service.py",
        runtime="py3",
        entry_module="square_service",
        export_mode="decorator",
        export_decorator="pycloud_export",
        dependency_allowlist=dependency_allowlist,
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
        # 步骤 2: 使用 GatewayServiceClient 调用
        print("[2] 使用 GatewayConnect（薄 HTTP 客户端）")
        print("-" * 60)

        with GatewayServiceClient(gateway_target, timeout_sec=10.0) as client:
            # 列出方法
            methods = client.list_methods(service_name=service_name, include_docs=False)
            print("可用方法:")
            for item in methods:
                print(f"  - {item.get('method')}")
            print()

            # 获取状态
            status = client.get_status(service_name=service_name)
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
            result1 = client.call(
                service_name=service_name,
                method="square",
                payload={"x": 7},
                timeout_sec=10.0,
            )
            print(f"  square(7) = {result1}")

            result2 = client.call(
                service_name=service_name,
                method="cube",
                payload={"x": 3},
                timeout_sec=10.0,
            )
            print(f"  cube(3) = {result2}")
        print()

        # 步骤 3: 使用 GatewayConnect 调用（module-like 语法）
        print("[3] 使用 GatewayConnect（模块化调用）")
        print("-" * 60)

        module_client = GatewayConnect(
            gateway_target,
            service_name=service_name,
            timeout_sec=10.0,
        )

        print(f"可用方法: {module_client.methods}")
        print()

        # 同步调用
        print("同步调用:")
        result3 = module_client.square.sync(x=9)
        print(f"  square(9) = {result3}")

        result4 = module_client.cube.sync(x=2)
        print(f"  cube(2) = {result4}")
        print()

        # 异步调用
        print("异步调用:")

        async def async_calls():
            result5 = await module_client.square(x=11)
            print(f"  square(11) = {result5}")

            result6 = await module_client.cube(x=4)
            print(f"  cube(4) = {result6}")

            # 并发调用
            results = await asyncio.gather(
                module_client.square(x=1),
                module_client.square(x=2),
                module_client.square(x=3),
            )
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
