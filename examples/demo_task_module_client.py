#!/usr/bin/env python3
"""
TaskSubmitter 演示

展示如何使用 TaskSubmitter 以模块化方式提交任务。
"""
import time

from pycloud_parallel import TaskSubmitter
from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2


def main():
    # 任务代码
    # 如果任务代码依赖节点未预装的包，可显式填 dependency_allowlist。
    dependency_allowlist = []
    job_suffix = int(time.time())
    blob = (
        b"def run(value, sleep_ms=0, should_fail=False):\n"
        b"    if sleep_ms > 0:\n"
        b"        import time\n"
        b"        time.sleep(sleep_ms / 1000.0)\n"
        b"    if should_fail:\n"
        b"        raise ValueError(f'intentional failure value={value}')\n"
        b"    return {'value': value, 'square': value * value}\n"
    )

    print("=" * 60)
    print("  TaskSubmitter 演示")
    print("=" * 60)
    print()

    # 创建任务客户端
    print("[1] 创建任务客户端...")
    print("-" * 60)

    task = TaskSubmitter.from_infocenter(
        infocenter_target="127.0.0.1:50051",
        blob=blob,
        runtime="py3",
        entry_module="task_demo",
        entry_callable="run",
        dependency_allowlist=dependency_allowlist,
        tags=["compute"],
        node_count=2,
        job_id=f"task-demo-{job_suffix}",
    )

    print(f"✓ 客户端创建成功")
    print(f"  client_id: {task.client_id}")
    print(f"  job_id: {task.job_id}")
    print(f"  code_version: {task.code_version}")
    print(f"  节点: {task.node_ids}")
    print()

    # 方式 1: 像调用函数一样提交任务并等待结果
    print("[2] 方式 1: 提交任务并等待结果（推荐）")
    print("-" * 60)

    # task.run(value=7) 自动提交并等待结果
    results = task.run(7)
    print(f"✓ 提交并等待完成:")
    for i, result in enumerate(results, 1):
        print(f"  [{i}] {result}")
    print()

    # 方式 2: 先提交，稍后获取结果
    print("[3] 方式 2: 先提交，稍后获取结果")
    print("-" * 60)

    # 提交任务
    resp = task.run.submit(9)
    print(f"✓ 任务已提交:")
    print(f"  accepted: {len(resp.accepted)}")
    for accepted in resp.accepted:
        print(f"    - task_id={accepted.task_id}")
    print(f"  rejected: {len(resp.rejected)}")
    print()

    # 等待结果
    print("等待结果...")
    results = task.wait_for_results(expected_count=len(resp.accepted), timeout_sec=10.0)
    print(f"✓ 获取到 {len(results)} 个结果:")
    for result in results:
        status_name = pb2.TaskStatus.Name(result.status)
        if result.result:
            from google.protobuf import json_format
            data = json_format.MessageToDict(result.result, preserving_proto_field_name=True)
            print(f"  task_id={result.task_id} status={status_name} result={data}")
    print()

    # 方式 3: 批量提交
    print("[4] 方式 3: 批量提交任务")
    print("-" * 60)

    # 使用 submit_payloads 批量提交
    payloads = [{"value": i} for i in range(1, 6)]
    resp = task.submit_payloads(payloads)
    print(f"✓ 批量提交 {len(payloads)} 个任务:")
    print(f"  accepted: {len(resp.accepted)}")
    print(f"  rejected: {len(resp.rejected)}")
    print()

    # 等待所有结果
    results = task.wait_for_results(expected_count=len(resp.accepted), timeout_sec=30.0)
    print(f"✓ 获取到 {len(results)} 个结果:")
    for result in results:
        status_name = pb2.TaskStatus.Name(result.status)
        if result.result:
            from google.protobuf import json_format
            data = json_format.MessageToDict(result.result, preserving_proto_field_name=True)
            print(f"  task_id={result.task_id} status={status_name} result={data}")
    print()

    # 查看节点指标
    print("[5] 节点指标:")
    print("-" * 60)
    metrics = task.get_metrics()
    for node_id, metric in metrics.items():
        print(f"  node_id={node_id}:")
        print(f"    queued={metric.queued} inflight={metric.inflight}")
        print(f"    running={metric.running} credit={metric.credit}")
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


if __name__ == "__main__":
    main()
