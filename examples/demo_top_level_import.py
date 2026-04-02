#!/usr/bin/env python3
"""
顶层导入演示

展示如何直接从 pycloud_parallel 导入模块化客户端。
"""

# 方式 1: 从 pycloud_parallel 直接导入（推荐）
from pycloud_parallel import GatewayConnect, DeployedService, TaskSubmitter

# 方式 2: 从子模块导入（仍然可用）
# from pycloud_parallel.controlplane import GatewayConnect, DeployedService, TaskSubmitter


def main():
    print("=" * 60)
    print("  顶层导入演示")
    print("=" * 60)
    print()

    print("✓ 成功从 pycloud_parallel 导入模块化客户端:")
    print(f"  - GatewayConnect: {GatewayConnect}")
    print(f"  - DeployedService: {DeployedService}")
    print(f"  - TaskSubmitter: {TaskSubmitter}")
    print()

    print("使用示例:")
    print()
    print("  # Service Session 模式")
    print("  from pycloud_parallel import DeployedService")
    print("  group = DeployedService.deploy_from_infocenter(...)")
    print("  result = await group.square(x=7)")
    print()
    print("  # Task 模式")
    print("  from pycloud_parallel import TaskSubmitter")
    print("  task = TaskSubmitter.from_infocenter(...)")
    print("  results = task.run(value=7)")
    print()
    print("  # Gateway 调用")
    print("  from pycloud_parallel import GatewayConnect")
    print("  client = GatewayConnect(..., service_name='my-service')")
    print("  result = client.square.sync(x=7)")
    print()

    print("=" * 60)
    print("  完成")
    print("=" * 60)


if __name__ == "__main__":
    main()