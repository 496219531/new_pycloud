#!/usr/bin/env python3
"""
PyCloud Gateway caller 示例。

演示如何通过 `Service.connect(..., transport="gateway")`
像本地模块一样调用远程服务。
"""

from pathlib import Path
import sys

REPO_SRC = Path(__file__).resolve().parents[1] / "src"
if str(REPO_SRC) not in sys.path:
    sys.path.insert(0, str(REPO_SRC))

import asyncio
import time

from pycloud_parallel import Service


def _service_exists(target: str, service_name: str) -> bool:
    try:
        with Service.connect(
            target=target,
            service_name=service_name,
            transport="gateway",
            timeout_sec=5.0,
            validate_on_init=False,
        ) as client:
            status = client.status()
            return status.get("route_count", 0) > 0
    except RuntimeError:
        return False


def _ensure_service(target: str, service_name: str):
    blob = (
        b"from pycloud_parallel import export\n\n"
        b"@export\n"
        b"def square(x=0, **_kwargs):\n"
        b"    x = int(x)\n"
        b"    return {'x': x, 'y': x * x}\n"
    )
    group = Service.deploy(
        infocenter_target=target,
        owner_client_id=f"gateway-module-demo-{int(time.time())}",
        service_name=service_name,
        source=blob,
        runtime="py3",
        entry_module="square_service",
        worker_count=1,
        tags=["compute"],
        min_success_nodes=1,
    )
    deadline = time.time() + 5.0
    while time.time() < deadline:
        if _service_exists(target, service_name):
            break
        time.sleep(0.2)
    return group


def main() -> None:
    service_name = f"square-service-{int(time.time())}"
    group = _ensure_service("127.0.0.1:50051", service_name)
    client = Service.connect(
        target="127.0.0.1:50051",
        service_name=service_name,
        transport="gateway",
        timeout_sec=10.0,
    )

    try:
        print("=" * 60)
        print("  PyCloud Service.connect(gateway) Demo")
        print("=" * 60)
        print()
        print(f"  Service: {service_name}")
        print(f"  Methods: {client.methods}")
        print()

        print("[+] sync call")
        print(
            "    square(x=7) -> "
            f"{client.square.sync(x=7)}"
        )
        print()

        async def _run() -> None:
            print("[+] async call")
            print(
                "    await square(x=11) -> "
                f"{await client.square(x=11)}"
            )

        asyncio.run(_run())
    finally:
        client.close()
        if group is not None:
            group.close(end_services=True, reason="demo_gateway_module_client cleanup")


if __name__ == "__main__":
    main()
