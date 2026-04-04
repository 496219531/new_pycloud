#!/usr/bin/env python3
"""
PyCloud Gateway module-like caller 示例。

演示如何像本地模块一样，通过 controlplane Gateway 调用远程服务。
"""

import asyncio

from pycloud_parallel import GatewayConnect


def main() -> None:
    client = GatewayConnect(
        "127.0.0.1:50051",
        service_name="compute-service",
        timeout_sec=10.0,
    )

    print("=" * 60)
    print("  PyCloud Gateway Module Client Demo")
    print("=" * 60)
    print()
    print(f"  Service: {client.service_name}")
    print(f"  Methods: {client.methods}")
    print()

    print("[+] sync call")
    print(f"    square.sync(x=7) -> {client.square.sync(x=7)}")
    print()

    async def _run() -> None:
        print("[+] async call")
        print(f"    await square(x=11) -> {await client.square(x=11)}")
        print()
        print("[+] generic async call")
        print(f"    await call('square', x=13) -> {await client.call('square', x=13)}")

    asyncio.run(_run())


if __name__ == "__main__":
    main()
