from pathlib import Path
import sys

REPO_SRC = Path(__file__).resolve().parents[1] / "src"
if str(REPO_SRC) not in sys.path:
    sys.path.insert(0, str(REPO_SRC))

import asyncio
import time
from pycloud_parallel import Service


async def main():
    # 部署服务（同步）
    # 如果服务依赖节点未预装的包，可显式填 dependency_allowlist。
    dependency_allowlist = []
    blob = (
        b"from pycloud_parallel import export\n\n"
        b"@export\n"
        b"def square(x=0, **_kwargs):\n"
        b"    x = int(x)\n"
        b"    return {'x': x, 'y': x * x}\n"
        b"@export\n"
        b"def cube(x=0, **_kwargs):\n"
        b"    x = int(x)\n"
        b"    return {'x': x, 'y': x * x * x}\n"
    )

    suffix = int(time.time())
    group = Service.deploy(
        infocenter_target="127.0.0.1:50051",
        owner_client_id=f"client-owner-{suffix}",
        service_name=f"square-service-{suffix}",
        blob=blob,
        runtime="py3",
        entry_module="square_service",
        entry_callable="square",
        export_mode="decorator",
        dependency_allowlist=dependency_allowlist,
        worker_count=4,
        heartbeat_timeout_sec=10,
        healthy_only=True,
        tags=["compute"],
        min_success_nodes=1,
        allow_partial=True,
        ensure_unique_service_name=True,
    )
    joined = False

    try:
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
                raise RuntimeError(f"call to node {node_id} failed: {exc}")
            print(f"节点 {node_id} 成功: {resp['data']}")

        # ✅ 高并发场景：批量异步调用
        async def batch_call():
            tasks = [
                group.acall_balanced("cube", {"x": i}, timeout_sec=10)
                for i in range(100)
            ]
            return await asyncio.gather(*tasks)

        results = await batch_call()
        for result in results:
            node_id, resp = result
            print(f"节点 {node_id}: {resp['data']}")

        print("若需长驻，可手动调用 group.join(...)")
        group.close(end_services=True, reason="grpc_async_demo cleanup")
        joined = True
    finally:
        group.close(
            end_services=False,
            reason="grpc_async_demo cleanup",
        )

asyncio.run(main())
