#!/usr/bin/env python3
"""
PyCloud 异步客户端示例

使用异步方式并发调用服务，适合高吞吐量场景。
"""
import asyncio
import time
from pycloud_parallel.controlplane.client import ServiceGroup


def main():
    suffix = int(time.time())
    # 同步方式（保留原有功能）
    # 如果服务依赖节点未预装的包，可显式填 dependency_allowlist。
    dependency_allowlist = []
    blob = (
        b"def pycloud_export(fn):\n"
        b"    fn.__pycloud_export__ = True\n"
        b"    return fn\n\n"
        b"@pycloud_export\n"
        b"def square(payload):\n"
        b"    x = int(payload.get('x', 0))\n"
        b"    return {'x': x, 'y': x * x}\n\n"
        b"@pycloud_export\n"
        b"def fibonacci(payload):\n"
        b"    n = int(payload.get('n', 0))\n"
        b"    if n <= 1:\n"
        b"        return n\n"
        b"    a, b = 0, 1\n"
        b"    for _ in range(n - 1):\n"
        b"        a, b = b, a + b\n"
        b"    return {'n': n, 'result': b}\n"
    )

    print("=" * 60)
    print("  PyCloud Async Client Demo")
    print("=" * 60)
    print()

    group = ServiceGroup.deploy_from_infocenter(
        infocenter_target="127.0.0.1:50051",
        owner_client_id=f"async-demo-{suffix}",
        service_name=f"compute-service-{suffix}",
        blob=blob,
        filename="compute.py",
        runtime="py3",
        entry_module="compute",
        entry_callable="square",
        export_mode="decorator",
        export_decorator="pycloud_export",
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

    try:
        # ================================================================
        # 示例 1: 单次异步调用
        # ================================================================
        print("-" * 60)
        print("  示例 1: 单次异步调用")
        print("-" * 60)

        async def demo_single_async_call():
            start = time.time()
            node_id, resp = await group.acall_balanced("square", {"x": 7}, timeout_sec=10)
            elapsed = time.time() - start
            print(f"    节点: {node_id}")
            print(f"    结果: {resp['data']}")
            print(f"    耗时: {elapsed:.3f}s")
            return node_id, resp

        asyncio.run(demo_single_async_call())
        print()

        # ================================================================
        # 示例 2: 批量并发调用
        # ================================================================
        print("-" * 60)
        print("  示例 2: 批量并发调用 (100 次)")
        print("-" * 60)

        async def demo_batch_calls():
            start = time.time()
            tasks = [
                group.acall_balanced("square", {"x": i}, timeout_sec=10)
                for i in range(100)
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            success = 0
            failed = 0
            for result in results:
                if isinstance(result, Exception):
                    failed += 1
                    print(f"    [FAIL] {result}")
                else:
                    success += 1

            elapsed = time.time() - start
            print(f"    成功: {success}, 失败: {failed}")
            print(f"    总耗时: {elapsed:.3f}s")
            print(f"    QPS: {success / elapsed:.1f}")
            return results

        asyncio.run(demo_batch_calls())
        print()

        # ================================================================
        # 示例 3: 调用所有节点
        # ================================================================
        print("-" * 60)
        print("  示例 3: 同时调用所有节点")
        print("-" * 60)

        async def demo_call_all_nodes():
            start = time.time()
            results = await group.acall_all(
                "square",
                {"x": 42},
                timeout_sec=10,
            )
            elapsed = time.time() - start

            for node_id, resp, exc in results:
                if exc:
                    print(f"    {node_id}: FAILED - {exc}")
                else:
                    print(f"    {node_id}: {resp['data']}")
            print(f"    总耗时: {elapsed:.3f}s")
            return results

        asyncio.run(demo_call_all_nodes())
        print()

        # ================================================================
        # 示例 4: 高并发压测
        # ================================================================
        print("-" * 60)
        print("  示例 4: 高并发压测 (1000 次)")
        print("-" * 60)

        async def demo_high_concurrency():
            start = time.time()

            async def one_call(i):
                try:
                    node_id, resp = await group.acall_balanced("square", {"x": i}, timeout_sec=30)
                    return (node_id, resp['data'], None)
                except Exception as e:
                    return (None, None, str(e))

            # 分批执行，每批 100 个
            batch_size = 100
            total = 1000
            success = 0
            failed = 0

            for batch_num in range(total // batch_size):
                tasks = [one_call(batch_num * batch_size + i) for i in range(batch_size)]
                results = await asyncio.gather(*tasks)
                for node_id, data, err in results:
                    if err:
                        failed += 1
                    else:
                        success += 1

                if (batch_num + 1) % 5 == 0:
                    elapsed = time.time() - start
                    print(f"    进度: {(batch_num + 1) * batch_size}/{total}, "
                          f"QPS: {success / elapsed:.1f}")

            elapsed = time.time() - start
            print(f"    成功: {success}, 失败: {failed}")
            print(f"    总耗时: {elapsed:.3f}s")
            print(f"    平均 QPS: {total / elapsed:.1f}")

        asyncio.run(demo_high_concurrency())
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
            reason="demo_async_client cleanup",
        )


if __name__ == "__main__":
    main()
