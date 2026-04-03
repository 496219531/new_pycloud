#!/usr/bin/env python3
"""
任务模式简化示例 - 展示自动 ID 生成

演示：
1. 所有 ID 自动生成（client_id, job_id, task_id）
2. 用户无需关心唯一性
3. 仍可手动指定（生产环境）
"""
import time
from pycloud_parallel.controlplane.client import TaskBatchClient
from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2


def demo_auto_ids():
    """演示自动生成所有 ID"""
    print("=" * 60)
    print("  方式 1：所有 ID 自动生成（推荐用于开发/测试）")
    print("=" * 60)
    print()

    blob = (
        b"def run(value):\n"
        b"    return {'value': value, 'square': value * value}\n"
    )

    # 不提供 client_id 和 job_id，系统自动生成
    with TaskBatchClient.from_infocenter(
        infocenter_target="127.0.0.1:50051",
        blob=blob,
        runtime="py3",
        entry_module="task_demo",
        entry_callable="run",
        tags=["compute"],
        node_count=2,
    ) as batch:
        print(f"✓ 自动生成的 client_id: {batch.client_id}")
        print(f"✓ 自动生成的 job_id: {batch.job_id}")
        print(f"  code_version: {batch.code_version}")
        print()

        # 提交任务（task_id 自动生成）
        result = batch.submit_payloads([
            {"value": 1},
            {"value": 2},
            {"value": 3},
        ])

        print(f"✓ 提交任务成功:")
        for accepted in result.accepted:
            print(f"  - task_id: {accepted.task_id}")
        print()

        # 等待结果
        results = list(batch.wait_for_results(
            expected_count=len(result.accepted),
            timeout_sec=10.0,
        ))

        print(f"✓ 获取结果:")
        for item in results:
            print(f"  - task_id={item.task_id} status={pb2.TaskStatus.Name(item.status)}")
        print()


def demo_manual_ids():
    """演示手动指定 ID（生产环境）"""
    print("=" * 60)
    print("  方式 2：手动指定 ID（推荐用于生产环境）")
    print("=" * 60)
    print()

    blob = (
        b"def run(payload):\n"
        b"    value = int(payload.get('value', 0))\n"
        b"    return {'value': value, 'square': value * value}\n"
    )

    # 手动指定 client_id 和 job_id，便于追踪
    with TaskBatchClient.from_infocenter(
        infocenter_target="127.0.0.1:50051",
        client_id="etl-worker-01",        # 手动指定
        job_id="data-load-20260330",      # 手动指定
        blob=blob,
        runtime="py3",
        entry_module="task_demo",
        entry_callable="run",
        tags=["compute"],
        node_count=2,
    ) as batch:
        print(f"✓ 使用指定的 client_id: {batch.client_id}")
        print(f"✓ 使用指定的 job_id: {batch.job_id}")
        print()

        # 提交任务
        result = batch.submit_payloads([{"value": 10}, {"value": 20}])

        print(f"✓ 提交任务成功:")
        for accepted in result.accepted:
            print(f"  - task_id: {accepted.task_id}")
        print()


def demo_uniqueness():
    """演示 ID 唯一性保证"""
    print("=" * 60)
    print("  方式 3：演示 ID 唯一性（多次创建实例）")
    print("=" * 60)
    print()

    blob = (
        b"def run(payload):\n"
        b"    return {'value': payload.get('value', 0)}\n"
    )

    # 创建多个实例，每个实例的 ID 都不同
    instances = []
    for i in range(3):
        batch = TaskBatchClient.from_infocenter(
            infocenter_target="127.0.0.1:50051",
            blob=blob,
            runtime="py3",
            entry_module="task_demo",
            entry_callable="run",
            tags=["compute"],
            node_count=2,
        )
        instances.append(batch)

        print(f"实例 {i+1}:")
        print(f"  client_id: {batch.client_id}")
        print(f"  job_id: {batch.job_id}")
        print()

    # 验证所有 ID 都是唯一的
    client_ids = [batch.client_id for batch in instances]
    job_ids = [batch.job_id for batch in instances]

    print("✓ ID 唯一性验证:")
    print(f"  client_id 唯一: {len(client_ids) == len(set(client_ids))}")
    print(f"  job_id 唯一: {len(job_ids) == len(set(job_ids))}")
    print()

    # 清理
    for batch in instances:
        batch.close()


def main():
    """主函数"""
    print()
    print("    PyCloud Task Batch Client - 自动 ID 生成示例")
    print("    ==============================================")
    print()

    # 方式 1：所有 ID 自动生成
    demo_auto_ids()

    # 方式 2：手动指定 ID
    demo_manual_ids()

    # 方式 3：演示唯一性
    demo_uniqueness()

    print("=" * 60)
    print("  完成")
    print("=" * 60)


if __name__ == "__main__":
    main()
