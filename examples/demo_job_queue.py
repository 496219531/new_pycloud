#!/usr/bin/env python3
"""
最小 Job Queue 演示。

提交一个大任务到唯一的 job-orchestrator service。
JobQueueClient 会先向 InfoCenter 查询 job-orchestrator route，再直连它自己的 HTTP 数据面。
job module 里同时定义：
1. `run`              子任务入口
2. `task_generator`   生成 payloads（可直接返回 list 或迭代器）
3. `update_globals`   可选；在 job-orch 端生成共享数据 dict
4. `apply_managed_globals`
                     可选；在 worker 端决定共享数据怎么作用到 runtime
4. `handle_result` / `handle_data`
                     收到每个结果时处理
5. `finalize`         收尾并产出最终结果
"""

from __future__ import annotations

import time

from pycloud_parallel import JobQueueClient


def main() -> None:
    target = "127.0.0.1:50051"

    job_blob = (
        b"JOB_CFG = None\n\n"
        b"def run(value=0, **_kwargs):\n"
        b"    value = int(value)\n"
        b"    return {'value': value, 'square': value * value, 'source': JOB_CFG['source']}\n\n"
        b"def task_generator(value=0, count=4, **_kwargs):\n"
        b"    return [{'value': value + i} for i in range(count)]\n\n"
        b"def update_globals(**_kwargs):\n"
        b"    return {'job_cfg': {'source': 'demo_job_queue'}}\n\n"
        b"def apply_managed_globals(values, **_context):\n"
        b"    return {'JOB_CFG': values.get('job_cfg')}\n\n"
        b"def handle_result(task_id, result, state=None, **_kwargs):\n"
        b"    state.setdefault('squares', []).append(result['square'])\n\n"
        b"def finalize(state=None, **_kwargs):\n"
        b"    values = state.get('squares', [])\n"
        b"    return {'count': len(values), 'sum_square': sum(values)}\n"
    )

    print("=" * 60)
    print("  Job Queue Demo")
    print("=" * 60)
    print(f"  InfoCenter: {target}")
    print()
    print("可选提交方式：")
    print("  1. submit_job_from_bytes(...)  适合直接提交 job module blob")
    print("  2. submit_job_from_module(...) 适合直接提交模块对象")
    print()

    with JobQueueClient(target, client_id=f"job-demo-{int(time.time())}", timeout_sec=10.0) as client:
        resp = client.submit_job_from_bytes(
            blob=job_blob,
            entry_module="job_demo",
            # job_payload={"value": 10, "count": 6},
            runtime="py3",
        )
        job = resp["job"]
        job_id = job["job_id"]
        print(f"submitted job_id={job_id} status={job['status']}")
        print()
        print("模块对象写法：")
        print(
            "client.submit_job_from_module("
            "module=my_job_module, "
            "job_payload={'value': 10, 'count': 6})"
        )
        print()

        deadline = time.time() + 30.0
        while time.time() < deadline:
            status = client.get_job_status(job_id)
            job = status["job"]
            print(
                f"status={job['status']} "
                f"results={len(job.get('results') or [])} "
                f"final_result={job.get('final_result')}"
            )
            if job["status"] in {"SUCCEEDED", "FAILED", "CANCELLED"}:
                print(job)
                break
            time.sleep(1.0)
        else:
            print("job did not finish before timeout")


if __name__ == "__main__":
    main()
