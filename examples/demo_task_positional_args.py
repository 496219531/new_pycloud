#!/usr/bin/env python3
"""
TaskSubmitter 位置参数演示

展示如何使用位置参数、命名参数或混合方式提交任务。
"""

from pycloud_parallel import TaskSubmitter
from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2


def main():
    print("=" * 60)
    print("  TaskSubmitter 位置参数演示")
    print("=" * 60)
    print()

    # 任务代码
    blob = (
        b"def run(payload):\n"
        b"    if isinstance(payload, dict):\n"
        b"        if 'args' in payload or 'kwargs' in payload:\n"
        b"            args = payload.get('args', [])\n"
        b"            kwargs = payload.get('kwargs', {})\n"
        b"            return compute(*args, **kwargs)\n"
        b"        else:\n"
        b"            x = payload.get('x', 0)\n"
        b"            y = payload.get('y', 0)\n"
        b"            return compute(x, y)\n"
        b"    return compute(0, 0)\n\n"
        b"def compute(x, y=1):\n"
        b"    return {'x': x, 'y': y, 'product': x * y}\n"
    )

    print("[1] 创建 TaskSubmitter...")
    print("-" * 60)

    task = TaskSubmitter.from_infocenter(
        infocenter_target="127.0.0.1:50051",
        blob=blob,
        runtime="py3",
        entry_module="task_demo",
        entry_callable="run",
        tags=["compute"],
        node_count=2,
    )

    print(f"✓ 客户端创建成功")
    print(f"  client_id: {task.client_id}")
    print(f"  job_id: {task.job_id}")
    print(f"  节点: {task.node_ids}")
    print()

    # 测试 1: 位置参数
    print("[2] 位置参数")
    print("-" * 60)
    results = task.run(7)
    print(f"task.run(7):")
    for result in results:
        status_name = pb2.TaskStatus.Name(result.status)
        if result.result:
            from google.protobuf import json_format
            data = json_format.MessageToDict(result.result, preserving_proto_field_name=True)
            print(f"  task_id={result.task_id} status={status_name} result={data}")
    print()

    # 测试 2: 命名参数
    print("[3] 命名参数")
    print("-" * 60)
    results = task.run(x=5, y=3)
    print(f"task.run(x=5, y=3):")
    for result in results:
        status_name = pb2.TaskStatus.Name(result.status)
        if result.result:
            from google.protobuf import json_format
            data = json_format.MessageToDict(result.result, preserving_proto_field_name=True)
            print(f"  task_id={result.task_id} status={status_name} result={data}")
    print()

    # 测试 3: 混合参数
    print("[4] 混合参数")
    print("-" * 60)
    results = task.run(10, y=2)
    print(f"task.run(10, y=2):")
    for result in results:
        status_name = pb2.TaskStatus.Name(result.status)
        if result.result:
            from google.protobuf import json_format
            data = json_format.MessageToDict(result.result, preserving_proto_field_name=True)
            print(f"  task_id={result.task_id} status={status_name} result={data}")
    print()

    # 测试 4: 批量提交
    print("[5] 批量提交（位置参数）")
    print("-" * 60)
    payloads = [{"args": [i]} for i in range(1, 6)]
    resp = task.submit_payloads(payloads)
    print(f"✓ 批量提交 {len(payloads)} 个任务:")
    print(f"  accepted: {len(resp.accepted)}")
    results = task.wait_for_results(expected_count=len(resp.accepted), timeout_sec=30.0)
    print(f"✓ 获取到 {len(results)} 个结果:")
    for result in results:
        status_name = pb2.TaskStatus.Name(result.status)
        if result.result:
            from google.protobuf import json_format
            data = json_format.MessageToDict(result.result, preserving_proto_field_name=True)
            print(f"  task_id={result.task_id} status={status_name} result={data}")
    print()

    # 清理
    print("[6] 清理")
    print("-" * 60)
    task.close()
    print("✓ 客户端已关闭")
    print()

    print("=" * 60)
    print("  完成")
    print("=" * 60)
    print()
    print("✅ TaskSubmitter 支持位置参数！")
    print()
    print("支持的参数传递方式：")
    print("  - 位置参数: task.run(7)")
    print("  - 命名参数: task.run(x=5, y=3)")
    print("  - 混合使用: task.run(10, y=2)")
    print()


if __name__ == "__main__":
    main()
