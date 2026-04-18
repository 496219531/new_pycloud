#!/usr/bin/env python3
"""
HTTP 风格兼容性演示

展示系统如何同时支持：
1. 新格式：{"args": [...], "kwargs": {...}}
2. HTTP 风格：{"key": value, ...}
"""

import asyncio
import sys
from pathlib import Path
from urllib.error import URLError

REPO_SRC = Path(__file__).resolve().parents[1] / "src"
if str(REPO_SRC) not in sys.path:
    sys.path.insert(0, str(REPO_SRC))

try:
    from pycloud_parallel import Service
except ModuleNotFoundError as exc:
    missing = str(getattr(exc, "name", "") or "")
    message = str(exc)
    if (
        missing in {"pycloud_parallel", "grpc", "cloudpickle", "google", "protobuf"}
        or missing.startswith("google.")
        or "Control-plane dependencies are missing." in message
        or "Local runtime dependencies are missing." in message
    ):
        raise SystemExit(
            "无法导入 pycloud_parallel 依赖。\n"
            f"当前解释器: {sys.executable}\n"
            "请使用已安装项目依赖的解释器运行，或先安装依赖后再执行。\n"
            "例如在这台机器上可优先尝试: python examples/demo_http_compat.py"
        ) from exc
    raise


def _exit_infocenter_unavailable(gateway_target: str, exc: BaseException) -> "NoReturn":
    raise SystemExit(
        "无法连接到 controlplane / InfoCenter。\n"
        f"目标地址: http://{gateway_target}\n"
        f"当前解释器: {sys.executable}\n"
        f"原始错误: {exc}\n"
        "请先启动本地服务:\n"
        "  ./scripts/start_services.sh start\n"
        "再重新运行该示例。"
    ) from exc


def main():
    gateway_target = "127.0.0.1:50051"
    service_name = "compat-demo"
    # 如果服务依赖节点未预装的包，可显式填 dependency_allowlist。
    dependency_allowlist = []

    print("=" * 60)
    print("  HTTP 风格兼容性演示")
    print("=" * 60)
    print()

    # 服务端代码：最自然的 Python 函数
    blob = (
        b"from pycloud_parallel import export\n\n"
        b"@export\n"
        b"def add(a, b):\n"
        b"    return {'a': a, 'b': b, 'sum': a + b}\n\n"
        b"@export\n"
        b"def greet(name, message='hello'):\n"
        b"    return {'greeting': f'{message}, {name}!'}\n"
    )

    # 部署服务
    print("[1] 部署服务...")
    print("-" * 60)

    try:
        group = Service.deploy(
            infocenter_target=gateway_target,
            service_name=service_name,
            source=blob,
            runtime="py3",
            entry_module="compat_demo",
            dependency_allowlist=dependency_allowlist,
            worker_count=2,
            tags=["compute"],
            min_success_nodes=1,
        )
    except (URLError, ConnectionError, OSError) as exc:
        _exit_infocenter_unavailable(gateway_target, exc)
    print(f"✓ 服务部署成功")
    print(f"  服务名: {group.service_name}")
    print()

    import time
    time.sleep(3)  # 等待服务启动

    try:
        print("[2] 测试不同的调用风格")
        print("-" * 60)
        print()

        # === 方式 1: 位置参数 ===
        print("1️⃣  位置参数 (新格式)")
        print("   调用: group.add.sync(10, 20)")
        result = group.add.sync(10, 20)
        print(f"   结果: {result}")
        print(f"   内部 payload: {{'args': [10, 20]}}")
        print()

        # === 方式 2: 命名参数 ===
        print("2️⃣  命名参数 (新格式)")
        print("   调用: group.add.sync(a=5, b=15)")
        result = group.add.sync(a=5, b=15)
        print(f"   结果: {result}")
        print(f"   内部 payload: {{'kwargs': {{'a': 5, 'b': 15}}}}")
        print()

        # === 方式 3: 混合参数 ===
        print("3️⃣  混合参数 (新格式)")
        print("   调用: group.add.sync(10, b=20)")
        result = group.add.sync(10, b=20)
        print(f"   结果: {result}")
        print(f"   内部 payload: {{'args': [10], 'kwargs': {{'b': 20}}}}")
        print()

        # === 方式 4: 通过 gateway transport 传普通 kwargs ===
        print("4️⃣  HTTP 风格 (直接传字典)")
        with Service.connect(
            target=gateway_target,
            service_name=service_name,
            transport="gateway",
            timeout_sec=10.0,
        ) as client:
            print("   调用: client.add.sync(a=100, b=200)")
            result = client.add.sync(a=100, b=200)
            print(f"   结果: {result}")
            print(f"   gateway transport 内部仍会把 {{'a': 100, 'b': 200}} 当作 kwargs 发送")
        print()

        # === 方式 5: 带默认值的位置参数 ===
        print("5️⃣  带默认值的位置参数")
        print("   调用: group.greet.sync('Alice')")
        result = group.greet.sync('Alice')
        print(f"   结果: {result}")
        print(f"   内部 payload: {{'args': ['Alice']}}")
        print()

        # === 方式 6: 带默认值的命名参数 ===
        print("6️⃣  带默认值的命名参数")
        print("   调用: group.greet.sync(name='Bob', message='hi')")
        result = group.greet.sync(name='Bob', message='hi')
        print(f"   结果: {result}")
        print(f"   内部 payload: {{'kwargs': {{'name': 'Bob', 'message': 'hi'}}}}")
        print()

        # === 方式 7: 异步调用 (位置参数) ===
        print("7️⃣  异步调用 (位置参数)")

        async def async_test():
            print("   调用: await group.add(1, 2)")
            result = await group.add(1, 2)
            print(f"   结果: {result}")

        asyncio.run(async_test())
        print()

        # === 方式 8: 批量并发 (混合风格) ===
        print("8️⃣  批量并发调用 (混合风格)")

        async def batch_test():
            print("   调用: asyncio.gather(")
            print("       group.add(1, 2),")
            print("       group.add(a=3, b=4),")
            print("       group.greet('Charlie'),")
            print("       group.greet(name='Dave', message='hey')")
            print("   )")
            results = await asyncio.gather(
                group.add(1, 2),
                group.add(a=3, b=4),
                group.greet('Charlie'),
                group.greet(name='Dave', message='hey'),
            )
            for i, r in enumerate(results, 1):
                print(f"   [{i}] {r}")

        asyncio.run(batch_test())
        print()

    finally:
        # 清理
        print("[3] 清理服务")
        print("-" * 60)
        group.close(end_services=True)
        print("✓ 服务已停止")
        print()

    print("=" * 60)
    print("  兼容性总结")
    print("=" * 60)
    print()
    print("✅ 系统支持以下 payload 格式：")
    print()
    print("1. 新格式 (位置参数):")
    print("   {'args': [10, 20]}")
    print("   → fn(10, 20)")
    print()
    print("2. 新格式 (命名参数):")
    print("   {'kwargs': {'a': 10, 'b': 20}}")
    print("   → fn(a=10, b=20)")
    print()
    print("3. 新格式 (混合参数):")
    print("   {'args': [10], 'kwargs': {'b': 20}}")
    print("   → fn(10, b=20)")
    print()
    print("4. HTTP 风格 (直接字典):")
    print("   {'a': 10, 'b': 20}")
    print("   → fn(a=10, b=20)")
    print()
    print("判断逻辑：")
    print("  - 如果 payload 包含 'args' 或 'kwargs' 键 → 使用新格式")
    print("  - 否则 → 整个 payload 作为 kwargs (HTTP 风格)")
    print()
    print("这样既支持新的位置参数，又完全兼容 HTTP 调用风格！")
    print()


if __name__ == "__main__":
    main()
