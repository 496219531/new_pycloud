#!/usr/bin/env python3
"""
顶层导入演示

展示 V1 顶层公开面。
"""

from pathlib import Path
import sys

REPO_SRC = Path(__file__).resolve().parents[1] / "src"
if str(REPO_SRC) not in sys.path:
    sys.path.insert(0, str(REPO_SRC))

from pycloud_parallel import DataRef, JobQueue, Service, TaskPool, export


def main():
    print("=" * 60)
    print("  V1 顶层导入演示")
    print("=" * 60)
    print()

    print("✓ 成功从 pycloud_parallel 导入 V1 公开面:")
    print(f"  - Service: {Service}")
    print(f"  - TaskPool: {TaskPool}")
    print(f"  - JobQueue: {JobQueue}")
    print(f"  - DataRef: {DataRef}")
    print(f"  - export: {export}")
    print()

    print("使用示例:")
    print()
    print("  # Service 模式")
    print("  from pycloud_parallel import Service")
    print("  group = Service.deploy(...)")
    print("  result = await group.square(x=7)")
    print()
    print("  # JobQueue 模式")
    print("  from pycloud_parallel import JobQueue")
    print("  client = JobQueue.connect(...)")
    print("  client.submit_job_from_bytes(...)")
    print()
    print("  # TaskPool 模式")
    print("  from pycloud_parallel import TaskPool")
    print("  pool = TaskPool.open(...)")
    print("  results = pool.wait_for_data(...)")
    print()
    print("  # 本地并行")
    print("  from pycloud_parallel.local import foreach, parallel_for")
    print("  print(foreach(lambda x: x * x, [1, 2, 3]))")
    print()

    print("=" * 60)
    print("  完成")
    print("=" * 60)


if __name__ == "__main__":
    main()
