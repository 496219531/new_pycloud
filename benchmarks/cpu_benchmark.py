from __future__ import annotations

"""中文说明：CPU 密集型基准脚本。

用于快速对比串行与并行的耗时，验证吞吐提升是否符合预期。
"""

import argparse
import time

from pc import ClusterConfig, ProjectConfig, RuntimeConfig, configure, foreach, project


def cpu_heavy(n: int) -> int:
    x = 0
    for i in range(80_000):
        x += (n * i) % 97
    return x


def run_serial(data):
    return [cpu_heavy(x) for x in data]


def run_parallel(data, clusters: int, capacity: int):
    # 基准默认关闭 Ray，先聚焦本机多进程与多“逻辑集群”调度路径。
    cfg = RuntimeConfig(
        clusters=[
            ClusterConfig(
                name=f"cluster-{i}",
                address="local",
                weight=1.0,
                capacity=capacity,
                use_ray=False,
            )
            for i in range(clusters)
        ],
        projects={"default": ProjectConfig(name="default", cpu_quota=clusters * capacity, mem_quota=0, priority=1)},
        default_project="default",
    )
    configure(config=cfg, reset=True)
    project("bench", cpu_quota=clusters * capacity, mem_quota=0, priority=1)
    return foreach(data, cpu_heavy, mode="ordered", chunk_size=1, project="bench")


def main():
    parser = argparse.ArgumentParser(description="CPU benchmark for pycloud-parallel")
    parser.add_argument("--size", type=int, default=200, help="number of tasks")
    parser.add_argument("--clusters", type=int, default=2, help="logical cluster count")
    parser.add_argument("--capacity", type=int, default=4, help="workers per cluster")
    args = parser.parse_args()

    data = list(range(args.size))

    t0 = time.perf_counter()
    serial = run_serial(data)
    t1 = time.perf_counter()
    parallel = run_parallel(data, clusters=args.clusters, capacity=args.capacity)
    t2 = time.perf_counter()

    if serial != parallel:
        raise RuntimeError("serial and parallel results mismatch")

    serial_time = t1 - t0
    parallel_time = t2 - t1
    speedup = serial_time / parallel_time if parallel_time > 0 else 0.0

    print(f"serial_time={serial_time:.3f}s")
    print(f"parallel_time={parallel_time:.3f}s")
    print(f"speedup={speedup:.2f}x")


if __name__ == "__main__":
    main()
