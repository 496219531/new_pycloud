#!/usr/bin/env python3
"""
任务模式 gRPC 示例。

演示：
1. 上传任务代码
2. 建立多节点流式任务会话
3. 带 runtime_key 提交一批任务
4. 用 CancelJob 取消另一批任务
5. 等待流式回传结果
"""

from __future__ import annotations

import time

from google.protobuf import json_format

from pycloud_parallel.controlplane.client import TaskBatchClient
from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2


def main() -> None:
    infocenter_target = "127.0.0.1:50051"
    client_id = f"task-demo-{int(time.time())}"
    run_job_id = f"job-run-{int(time.time())}"
    cancel_job_id = f"job-cancel-{int(time.time())}"
    runtime_key = "demo-runtime-v1"
    # 如果任务代码依赖节点未预装的包，可显式填写白名单。
    # 例如: ["./third_party/my_local_pkg", "/abs/path/to/pkg.whl", "orjson==3.10.18"]
    dependency_allowlist = []

    blob = (
        b"def run(payload):\n"
        b"    value = int(payload.get('value', 0))\n"
        b"    sleep_ms = int(payload.get('sleep_ms', 0))\n"
        b"    if sleep_ms > 0:\n"
        b"        import time\n"
        b"        time.sleep(sleep_ms / 1000.0)\n"
        b"    if payload.get('should_fail'):\n"
        b"        raise ValueError(f'intentional failure value={value}')\n"
        b"    return {'value': value, 'square': value * value}\n"
    )

    print("=" * 60)
    print("  PyCloud Task Client Demo")
    print("=" * 60)
    print(f"  InfoCenter:  {infocenter_target}")
    print(f"  client_id:   {client_id}")
    print()

    with TaskBatchClient.from_infocenter(
        infocenter_target=infocenter_target,
        client_id=client_id,
        job_id=run_job_id,
        blob=blob,
        filename="task_demo.py",
        runtime="py3",
        entry_module="task_demo",
        entry_callable="run",
        dependency_allowlist=dependency_allowlist,
        tags=["compute"],
        node_count=2,
        node_limit=50,
        timeout_sec=10.0,
    ) as batch:
        print("[0] 选中任务节点:")
        for node_id in batch.node_ids:
            node = batch.nodes[node_id]
            print(
                "    "
                f"node_id={node.node_id} control_addr={node.control_addr} "
                f"credit={node.credit} queued={node.queued} inflight={node.inflight}"
            )

        print(f"[1] 上传完成 code_version={batch.code_version}")

        submit = batch.submit_tasks(
            [
                pb2.TaskSubmitItem(task_id=f"{run_job_id}-1", payload={"value": 2}, priority=1, runtime_key=runtime_key),
                pb2.TaskSubmitItem(task_id=f"{run_job_id}-2", payload={"value": 3, "should_fail": True}, priority=1, runtime_key=runtime_key),
            ],
            job_id=run_job_id,
        )
        print(f"[2] 已提交运行批次 job_id={run_job_id}, runtime_key={runtime_key}, accepted={len(submit.accepted)}")

        cancel_submit = batch.submit_tasks(
            [
                pb2.TaskSubmitItem(task_id=f"{cancel_job_id}-1", payload={"value": 10, "sleep_ms": 3000}, priority=1, runtime_key=runtime_key),
                pb2.TaskSubmitItem(task_id=f"{cancel_job_id}-2", payload={"value": 11, "sleep_ms": 3000}, priority=1, runtime_key=runtime_key),
            ],
            execution_mode=pb2.EXECUTION_MODE_PERSISTENT,
            job_id=cancel_job_id,
        )
        print(f"[3] 已提交待取消批次 job_id={cancel_job_id}, accepted={len(cancel_submit.accepted)}")

        cancel = batch.cancel_job(reason="demo cancel job", job_id=cancel_job_id)
        print(
            "[4] CancelJob 完成 "
            f"queued_cancelled={cancel.queued_cancelled} "
            f"running_marked={cancel.running_marked} "
            f"already_done={cancel.already_done} "
            f"not_found={cancel.not_found}"
        )

        run_results = list(batch.wait_for_results(job_id=run_job_id, expected_count=2, timeout_sec=10.0, wait_ms=500, limit=20))
        cancel_results = list(batch.wait_for_results(job_id=cancel_job_id, expected_count=2, timeout_sec=10.0, wait_ms=500, limit=20))
        results = run_results + cancel_results

        print("[5] 拉取结果：")
        if not results:
            print("    暂无结果，确认 node 已启动且允许内部执行")
        for item in results:
            detail = {}
            if item.result:
                detail = json_format.MessageToDict(item.result, preserving_proto_field_name=True)
            elif item.error:
                detail = {"error_type": item.error.type, "error_message": item.error.message}
            print(
                "    "
                f"task_id={item.task_id} "
                f"job_id={item.job_id} "
                f"status={pb2.TaskStatus.Name(item.status)} "
                f"detail={detail}"
            )

        print("[6] 节点指标:")
        for node_id, metrics in batch.get_metrics().items():
            print(
                "    "
                f"node_id={node_id} "
                f"queued={metrics.queued} inflight={metrics.inflight} "
                f"running={metrics.running} credit={metrics.credit}"
            )


if __name__ == "__main__":
    main()
