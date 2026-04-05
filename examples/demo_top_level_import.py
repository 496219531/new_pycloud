#!/usr/bin/env python3
"""
顶层导入演示

展示如何直接从 pycloud_parallel 导入模块化客户端。
"""

# 方式 1: 从 pycloud_parallel 直接导入（推荐）
from pycloud_parallel import GatewayConnect, DeployedService, JobQueueClient, TaskPoolSession

# 方式 2: 从子模块导入（仍然可用）
# from pycloud_parallel.controlplane import GatewayConnect, DeployedService, JobQueueClient, TaskPoolSession


def main():
    print("=" * 60)
    print("  顶层导入演示")
    print("=" * 60)
    print()

    print("✓ 成功从 pycloud_parallel 导入模块化客户端:")
    print(f"  - GatewayConnect: {GatewayConnect}")
    print(f"  - DeployedService: {DeployedService}")
    print(f"  - JobQueueClient: {JobQueueClient}")
    print(f"  - TaskPoolSession: {TaskPoolSession}")
    print()

    print("使用示例:")
    print()
    print("  # Service Session 模式")
    print("  from pycloud_parallel import DeployedService")
    print("  group = DeployedService.deploy_from_infocenter(...)")
    print("  result = await group.square(x=7)")
    print()
    print("  # JobQueue 模式")
    print("  from pycloud_parallel import JobQueueClient")
    print("  client = JobQueueClient(...)")
    print("  client.submit_job_from_bytes(...)")
    print()
    print("  # TaskPool 模式")
    print("  from pycloud_parallel import TaskPoolSession")
    print("  pool = TaskPoolSession.from_infocenter(...)")
    print("  results = pool.wait_for_data(...)")
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
