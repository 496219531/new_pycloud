from pycloud_parallel.controlplane.client import MultiNodeServiceGroup
import asyncio

async def main():
    # 部署服务（同步）
    blob = (
        b"def pycloud_export(fn):\n"
        b"    fn.__pycloud_export__ = True\n"
        b"    return fn\n\n"
        b"@pycloud_export\n"
        b"def square(payload):\n"
        b"    x = int(payload.get('x', 0))\n"
        b"    return {'x': x, 'y': x * x}\n"
        b"@pycloud_export\n"
        b"def cube(payload):\n"
        b"    x = int(payload.get('x', 0))\n"
        b"    return {'x': x, 'y': x * x * x}\n"
    )

    group = MultiNodeServiceGroup.deploy_from_infocenter(
        infocenter_target="127.0.0.1:50051",
        owner_client_id="client-owner-001",
        service_name="square-service",          # 同名已存在会直接抛错
        blob=blob,
        filename="square_service.py",
        runtime="py3.11",
        entry_module="square_service",
        entry_callable="square",
        export_mode="decorator",
        export_decorator="pycloud_export",
        worker_count=4,
        heartbeat_timeout_sec=30,
        healthy_only=True,
        tags=["compute"],                       # 可按节点标签筛选
        min_success_nodes=1,
        allow_partial=True,
        ensure_unique_service_name=True,        # 默认就是 True
    )

    group.start_keepalive()

    # ✅ 单次异步调用
    node_id, resp = await group.acall_balanced(
        "square",
        {"x": 7},
        timeout_sec=10
    )
    print(f"节点 {node_id}: {resp['data']}")

    # ✅ 批量并发调用所有节点
    results = await group.acall_all(
        "square",
        {"x": 100},  # 单个 payload 发送给所有节点
        timeout_sec=10,
        max_concurrency=50
    )
    for node_id, resp, exc in results:
        if exc:
            print(f"节点 {node_id} 失败: {exc}")
        else:
            print(f"节点 {node_id} 成功: {resp['data']}")

    # ✅ 高并发场景：批量异步调用
    async def batch_call():
        tasks = [
            group.acall_balanced("cube", {"x": i}, timeout_sec=10)
            for i in range(1000)
        ]
        return await asyncio.gather(*tasks, return_exceptions=True)
    
    results = await batch_call()
    for result in results:
        if isinstance(result, Exception):
            print(f"失败: {result}")
        else:
            node_id, resp = result
            print(f"节点 {node_id}: {resp['data']}")

    group.close()

asyncio.run(main())
