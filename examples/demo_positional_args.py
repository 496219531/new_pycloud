#!/usr/bin/env python3
"""
位置参数演示

展示如何使用位置参数、命名参数或混合方式调用服务。
"""

import asyncio
import time
from pycloud_parallel import Service


def main():
    gateway_target = "127.0.0.1:50051"
    service_name = f"args-demo-service-{int(time.time())}"

    print("=" * 60)
    print("  位置参数演示")
    print("=" * 60)
    print()

    # 定义服务代码，展示不同参数风格的函数
    blob = (
        b"from pycloud_parallel import export\n\n"
        b"@export\n"
        b"def square(x):\n"
        b"    return {'x': x, 'square': x * x}\n\n"
        b"@export\n"
        b"def add(a, b):\n"
        b"    return {'a': a, 'b': b, 'sum': a + b}\n\n"
        b"@export\n"
        b"def power(base, exponent=2):\n"
        b"    return {'base': base, 'exponent': exponent, 'result': base ** exponent}\n\n"
        b"@export\n"
        b"def summarize(*values):\n"
        b"    return {'count': len(values), 'sum': sum(values), 'values': list(values)}\n\n"
        b"@export\n"
        b"def compute(a, b, c=0, d=0):\n"
        b"    return {'a': a, 'b': b, 'c': c, 'd': d, 'total': a + b + c + d}\n"
    )

    # 步骤 1: 部署服务
    print("[1] 部署服务...")
    print("-" * 60)

    group = Service.deploy_from_infocenter(
        infocenter_target=gateway_target,
        service_name=service_name,
        blob=blob,
        runtime="py3",
        entry_module="args_demo",
        export_mode="decorator",
        worker_count=2,
        tags=["compute"],
        min_success_nodes=1,
    )
    print(f"✓ 服务部署成功")
    print(f"  服务名: {group.service_name}")
    print(f"  节点: {list(group.sessions.keys())}")
    print()

    time.sleep(3)  # 等待服务启动

    try:
        # 步骤 2: 测试不同参数传递方式
        print("[2] 测试参数传递方式")
        print("-" * 60)
        print()

        # 测试 1: 单参数 - 位置参数
        print("1️⃣ 单参数函数 (位置参数)")
        result = group.square.sync(7)
        print(f"   square(7) = {result}")
        print()

        # 测试 2: 单参数 - 命名参数
        print("2️⃣ 单参数函数 (命名参数)")
        result = group.square.sync(x=9)
        print(f"   square(x=9) = {result}")
        print()

        # 测试 3: 多参数 - 全部位置参数
        print("3️⃣ 多参数函数 (全部位置参数)")
        result = group.add.sync(10, 20)
        print(f"   add(10, 20) = {result}")
        print()

        # 测试 4: 多参数 - 全部命名参数
        print("4️⃣ 多参数函数 (全部命名参数)")
        result = group.add.sync(a=5, b=15)
        print(f"   add(a=5, b=15) = {result}")
        print()

        # 测试 5: 带默认值 - 只传位置参数
        print("5️⃣ 带默认值函数 (只传必需参数)")
        result = group.power.sync(3)
        print(f"   power(3) = {result}")
        print()

        # 测试 6: 带默认值 - 位置参数 + 命名参数
        print("6️⃣ 带默认值函数 (位置参数 + 命名参数)")
        result = group.power.sync(2, exponent=10)
        print(f"   power(2, exponent=10) = {result}")
        print()

        # 测试 7: 可变参数 - 多个位置参数
        print("7️⃣ 可变参数函数 (多个位置参数)")
        result = group.summarize.sync(1, 2, 3, 4, 5)
        print(f"   summarize(1, 2, 3, 4, 5) = {result}")
        print()

        # 测试 8: 混合参数 - 位置 + 命名
        print("8️⃣ 混合参数函数 (位置 + 命名)")
        result = group.compute.sync(1, 2, c=3, d=4)
        print(f"   compute(1, 2, c=3, d=4) = {result}")
        print()

        # 测试 9: 异步调用
        print("9️⃣ 异步调用 (位置参数)")

        async def async_test():
            result = await group.square(11)
            print(f"   await square(11) = {result}")

        asyncio.run(async_test())
        print()

        # 测试 10: 批量并发
        print("🔟 批量并发调用")

        async def batch_test():
            results = await asyncio.gather(
                group.square(1),
                group.square(2),
                group.square(3),
                group.add(10, 20),
            )
            for i, r in enumerate(results, 1):
                print(f"   [{i}] {r}")

        asyncio.run(batch_test())
        print()

    finally:
        # 步骤 3: 清理
        print("[3] 清理服务")
        print("-" * 60)
        group.close(end_services=True)
        print("✓ 服务已停止")
        print()

    print("=" * 60)
    print("  完成")
    print("=" * 60)
    print()
    print("✅ 所有测试通过！")
    print()
    print("支持的参数传递方式：")
    print("  - 位置参数: func(1, 2, 3)")
    print("  - 命名参数: func(a=1, b=2)")
    print("  - 混合使用: func(1, 2, c=3, d=4)")
    print("  - 同步调用: func.sync(1, 2)")
    print("  - 异步调用: await func(1, 2)")
    print()


if __name__ == "__main__":
    main()
