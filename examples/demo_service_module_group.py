#!/usr/bin/env python3
"""
PyCloud 模块化客户端示例

像使用 Python 模块一样调用远程服务，简单直观。
"""
import asyncio
import time
from pycloud_parallel import Service


def main():
    # 服务代码
    # 如果服务依赖节点未预装的包，可显式填 dependency_allowlist。
    dependency_allowlist = []
    blob = (
        b"from pycloud_parallel import export\n\n"
        b"@export\n"
        b"def square(x=0, **_kwargs):\n"
        b"    return {'x': x, 'y': x * x}\n\n"
        b"@export\n"
        b"def fibonacci(n=0, **_kwargs):\n"
        b"    if n <= 1:\n"
        b"        return n\n"
        b"    a, b = 0, 1\n"
        b"    for _ in range(n - 1):\n"
        b"        a, b = b, a + b\n"
        b"    return {'n': n, 'result': b}\n\n"
        b"@export\n"
        b"def slow_add(a=0, b=0, **_kwargs):\n"
        b"    import time\n"
        b"    time.sleep(0.1)\n"
        b"    return {'a': a, 'b': b, 'result': a + b}\n"
    )

    print("=" * 60)
    print("  PyCloud Module-Like Client Demo")
    print("  像调用本地函数一样调用远程服务")
    print("=" * 60)
    print()

    # V1 公开入口使用 Service。
    import time
    group = Service.deploy_from_infocenter(
        infocenter_target="127.0.0.1:50051",
        owner_client_id=f"module-demo-{int(time.time())}",
        service_name=f"compute-service-1",
        blob=blob,
        runtime="py3",
        entry_module="compute",
        entry_callable="square",
        export_mode="decorator",
        dependency_allowlist=dependency_allowlist,
        worker_count=4,
        heartbeat_timeout_sec=30,
        healthy_only=True,
        tags=["compute"],
        min_success_nodes=1,
        allow_partial=True,
    )
    joined = False

    print(f"[+] Service deployed on nodes: {list(group.sessions.keys())}")
    print(f"[+] HTTP endpoints:")
    for node_id, session in group.sessions.items():
        print(f"    {node_id}: {session.http_base_url}")
    print()

    # 打印可用方法
    print(f"[+] Available methods: {group.methods}")
    print()

    try:
        # ================================================================
        # 示例 1: 简单的异步调用（像调用本地函数一样）
        # ================================================================
        print("-" * 60)
        print("  示例 1: 简单的异步调用（像调用本地函数一样）")
        print("-" * 60)

        async def demo_simple_call():
            # 就像调用本地函数一样！
            result = await group.square(x=7)
            print(f"    group.square(x=7) = {result}")

            result = await group.fibonacci(n=10)
            print(f"    group.fibonacci(n=10) = {result}")

        asyncio.run(demo_simple_call())
        print()

        # ================================================================
        # 示例 2: 批量并发调用
        # ================================================================
        print("-" * 60)
        print("  示例 2: 批量并发调用 (100 次)")
        print("-" * 60)

        async def demo_batch_calls():
            start = time.time()

            # 批量调用，就像本地一样简单
            tasks = [
                group.square(x=i) for i in range(100)
            ]
            results = await asyncio.gather(*tasks)

            elapsed = time.time() - start
            success = len(results)
            print(f"    成功: {success}, 失败: 0")
            print(f"    总耗时: {elapsed:.3f}s")
            print(f"    QPS: {success / elapsed:.1f}")

        asyncio.run(demo_batch_calls())
        print()

        # ================================================================
        # 示例 3: 同步调用
        # ================================================================
        print("-" * 60)
        print("  示例 3: 同步调用")
        print("-" * 60)

        # 不需要 async/await，直接调用
        result = group.square.sync(x=5)
        print(f"    group.square.sync(x=5) = {result}")

        result = group.fibonacci.sync(n=20)
        print(f"    group.fibonacci.sync(n=20) = {result}")
        print()

        # ================================================================
        # 示例 4: 广播调用（同时调用所有节点）
        # ================================================================
        print("-" * 60)
        print("  示例 4: 广播调用（同时调用所有节点）")
        print("-" * 60)

        async def demo_broadcast():
            # 同时调用所有节点，比较结果
            results = await group.square.broadcast(x=42)

            print(f"    group.square.broadcast(x=42):")
            for node_id, result, error in results:
                if error:
                    raise RuntimeError(f"broadcast to node {node_id} failed: {error}")
                print(f"      {node_id}: {result}")

        asyncio.run(demo_broadcast())
        print()

        # ================================================================
        # 示例 5: 自定义调用选项
        # ================================================================
        print("-" * 60)
        print("  示例 5: 自定义调用选项")
        print("-" * 60)

        async def demo_custom_options():
            # 自定义超时和策略
            result = await group.square.with_options(
                timeout_sec=30,
                strategy="round_robin",
            )(x=100)
            print(f"    custom options: {result}")

        asyncio.run(demo_custom_options())
        print()

        # ================================================================
        # 示例 6: 高并发压测
        # ================================================================
        print("-" * 60)
        print("  示例 6: 高并发压测 (1000 次)")
        print("-" * 60)

        async def demo_high_concurrency():
            start = time.time()

            # 分批执行
            batch_size = 100
            total = 1000
            success = 0

            for batch_num in range(total // batch_size):
                tasks = [group.square(x=batch_num * batch_size + i) for i in range(batch_size)]
                results = await asyncio.gather(*tasks)
                success += len(results)

                if (batch_num + 1) % 5 == 0:
                    elapsed = time.time() - start
                    print(f"    进度: {(batch_num + 1) * batch_size}/{total}, "
                          f"QPS: {success / elapsed:.1f}")

            elapsed = time.time() - start
            print(f"    成功: {success}, 失败: 0")
            print(f"    总耗时: {elapsed:.3f}s")
            print(f"    平均 QPS: {total / elapsed:.1f}")

        asyncio.run(demo_high_concurrency())
        print()

        # ================================================================
        # 示例 7: 使用通用 call 接口
        # ================================================================
        print("-" * 60)
        print("  示例 7: 使用通用 call 接口")
        print("-" * 60)

        async def demo_generic_call():
            # 不确定方法名时使用通用接口
            result = await group.call("square", x=999)
            print(f"    group.call('square', x=999) = {result}")

            result = group.call_sync("fibonacci", n=15)
            print(f"    group.call_sync('fibonacci', n=15) = {result}")

        asyncio.run(demo_generic_call())
        print()

        print("=" * 60)
        print("  Demo 完成!")
        print("=" * 60)
        print("  服务进入长驻模式，按 Ctrl+C 自动回收")
        print("=" * 60)
        print()
        group.join(
            end_services_on_interrupt=True,
            end_reason="owner ctrl+c",
        )
        joined = True

    finally:
        group.close(
            end_services=not joined,
            reason="demo_service_module_group cleanup",
        )


if __name__ == "__main__":
    main()
