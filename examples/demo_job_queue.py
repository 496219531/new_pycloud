#!/usr/bin/env python3
"""
最小 Job Queue 演示。

提交一个大任务到 controlplane 队列中。
当它排到执行时，会先运行 driver 代码生成 subtasks，
再把 subtasks 交给当前 Task Pool 并行执行。
"""

from __future__ import annotations

import time

from pycloud_parallel import JobQueueClient


def main() -> None:
    target = "127.0.0.1:50051"

    driver_blob = (
        b"def build(value=0, count=8, **_kwargs):\n"
        b"    return [{'value': value + i} for i in range(count)]\n"
    )
    task_blob = (
        b"def run(value=0, **_kwargs):\n"
        b"    value = int(value)\n"
        b"    return {'value': value, 'square': value * value}\n"
    )

    def build_from_func(value=0, count=4, **_kwargs):
        return [{"value": value + i} for i in range(count)]

    def run_task(value=0, **_kwargs):
        value = int(value)
        return {"value": value, "square": value * value}

    print("=" * 60)
    print("  Job Queue Demo")
    print("=" * 60)
    print(f"  ControlPlane: {target}")
    print()
    print("可选提交方式：")
    print("  1. submit_job_from_bytes(...)  适合显式控制 driver / task blob")
    print("  2. submit_job_from_func(...)   适合直接提交函数对象")
    print("  3. submit_job_from_module(...) 适合直接提交模块对象")
    print()

    with JobQueueClient(target, timeout_sec=10.0) as client:
        resp = client.submit_job_from_bytes(
            blob=driver_blob,
            driver_entry_module="job_driver_demo",
            driver_entry_callable="build",
            driver_payload={"value": 10, "count": 6},
            client_id=f"job-demo-{int(time.time())}",
            runtime="py3",
            task_blob=task_blob,
            task_entry_module="task_demo",
            task_entry_callable="run",
            task_package_format="py",
            tags=["compute"],
            node_count=2,
            pool_name="demo-job-pool",
            pool_worker_count=2,
            pool_node_count=2,
            pool_heartbeat_timeout_sec=30,
            priority=5,
        )
        job = resp["job"]
        job_id = job["job_id"]
        print(f"submitted job_id={job_id} status={job['status']}")
        print()
        print("等价的函数对象写法：")
        print(
            "client.submit_job_from_func("
            "func=build_from_func, "
            "task_func=run_task, "
            "pool_worker_count=2, pool_node_count=2)"
        )
        print()
        print("模块对象写法：")
        print(
            "client.submit_job_from_module("
            "module=my_driver_module, "
            "task_module=my_task_module, "
            "task_entry_callable='run')"
        )
        print()

        deadline = time.time() + 30.0
        while time.time() < deadline:
            status = client.get_job_status(job_id)
            job = status["job"]
            print(f"status={job['status']} results={len(job.get('results') or [])}")
            if job["status"] in {"SUCCEEDED", "FAILED", "CANCELLED"}:
                print(job)
                break
            time.sleep(1.0)
        else:
            print("job did not finish before timeout")


if __name__ == "__main__":
    main()
