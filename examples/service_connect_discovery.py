#!/usr/bin/env python3
"""
调用已部署服务示例。

这个脚本演示通过 `Service.connect(..., transport="discovery")`
直接按 discovery transport 调已有服务。
"""

from __future__ import annotations

from pathlib import Path
import sys
import time

REPO_SRC = Path(__file__).resolve().parents[1] / "src"
if str(REPO_SRC) not in sys.path:
    sys.path.insert(0, str(REPO_SRC))

import asyncio

from pycloud_parallel import Service


def main() -> None:
    infocenter_target = "127.0.0.1:50051"
    service_name = f"square-service-{int(time.time())}"
    blob = (
        b"from pycloud_parallel import export\n\n"
        b"@export\n"
        b"def square(x=0, **_kwargs):\n"
        b"    x = int(x)\n"
        b"    return {'x': x, 'y': x * x}\n"
    )

    print("=" * 60)
    print("  PyCloud Service.connect(discovery) Demo")
    print("=" * 60)
    print()
    print(f"  Discovery Target: {infocenter_target}")
    print(f"  Service: {service_name}")
    print()

    group = Service.deploy(
        target=infocenter_target,
        owner_client_id=f"discovery-demo-{int(time.time())}",
        service_name=service_name,
        source=blob,
        runtime="py3",
        worker_count=1,
        tags=["compute"],
        min_success_nodes=1,
    )

    module_client = Service.connect(
        target=infocenter_target,
        service_name=service_name,
        timeout_sec=10.0,
        transport="discovery",
    )
    try:
        print(f"[+] Status: {module_client.status()}")
        print(f"[+] Methods: {module_client.methods}")
        print(f"[+] sync call: {module_client.square.sync(x=9)}")

        async def _run() -> None:
            result = await module_client.square(x=11)
            print(f"[+] async call: {result}")

        asyncio.run(_run())
    finally:
        module_client.close()
        group.close(end_services=True, reason="grpc_existing_service_client_demo cleanup")


if __name__ == "__main__":
    main()
