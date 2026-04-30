#!/usr/bin/env python3
"""
最小 Job Queue 演示。

提交一个大任务到唯一的 job-orchestrator service。
JobQueue 会先向 InfoCenter 查询 job-orchestrator route，再直连它自己的 HTTP 数据面。
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

import importlib.util
from pathlib import Path
import sys
import tempfile

REPO_SRC = Path(__file__).resolve().parents[1] / "src"
if str(REPO_SRC) not in sys.path:
    sys.path.insert(0, str(REPO_SRC))

import time
from pycloud_parallel import JobQueue


def main() -> None:
    target = "127.0.0.1:50051"

    job_source = (
        "JOB_CFG = None\n\n"
        "def run(value=0, **_kwargs):\n"
        "    value = int(value)\n"
        "    cfg = JOB_CFG or {}\n"
        "    return {'value': value, 'square': value * value, 'source': cfg.get('source', 'unknown')}\n\n"
        "def task_generator(value=0, count=4, **_kwargs):\n"
        "    return [{'value': value + i} for i in range(count)]\n\n"
        "def update_globals(**_kwargs):\n"
        "    return {'job_cfg': {'source': 'demo_job_queue'}}\n\n"
        "def apply_managed_globals(values, **_context):\n"
        "    global JOB_CFG\n"
        "    JOB_CFG = values.get('job_cfg')\n\n"
        "def handle_result(index, result, state=None, **_kwargs):\n"
        "    state.setdefault('squares', []).append(result['square'])\n\n"
        "def finalize(state=None, **_kwargs):\n"
        "    values = state.get('squares', [])\n"
        "    return {'count': len(values), 'sum_square': sum(values)}\n"
    )

    print("=" * 60)
    print("  Job Queue Demo")
    print("=" * 60)
    print(f"  InfoCenter: {target}")
    print()
    print("提交方式：")
    print("  submit(source=job_module, ...)       推荐，直接提交模块对象")
    print("  submit_job_from_bytes(...)           仍可用，但仅作为兼容/高级路径")
    print()

    with tempfile.TemporaryDirectory(prefix="pycloud-job-demo-") as tmpdir:
        module_path = Path(tmpdir) / "job_demo.py"
        module_path.write_text(job_source, encoding="utf-8")
        spec = importlib.util.spec_from_file_location("job_demo", module_path)
        if spec is None or spec.loader is None:
            raise RuntimeError(f"failed to load job module from {module_path}")
        job_module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = job_module
        spec.loader.exec_module(job_module)

        with JobQueue.connect(target, client_id=f"job-demo-{int(time.time())}", timeout_sec=10.0) as client:
            resp = client.submit(
                source=job_module,
                job_payload={"value": 10, "count": 6},
            )
            job = resp["job"]
            job_id = job["job_id"]
            print(f"submitted job_id={job_id} status={job['status']}")
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
