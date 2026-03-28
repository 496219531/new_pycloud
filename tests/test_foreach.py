"""中文说明：验证 foreach 的核心语义。

覆盖点：保序输出、as_completed 输出、失败跳过、多集群分发。
"""

import time

from pc import ClusterConfig, ProjectConfig, RuntimeConfig, configure, foreach, project
from pycloud_parallel.runtime import get_runtime


def _square_or_fail(x):
    if x in (3, 7):
        raise ValueError("boom")
    return x * x


def _sleep_inverse(x):
    time.sleep((5 - x) * 0.02)
    return x


def _square(x):
    return x * x


def _setup_runtime(num_clusters=1, capacity=4):
    clusters = [
        ClusterConfig(name=f"cluster-{i}", address="local", weight=1.0, capacity=capacity, use_ray=False)
        for i in range(num_clusters)
    ]
    projects = {"default": ProjectConfig(name="default", cpu_quota=max(2, capacity), mem_quota=0, priority=1)}
    cfg = RuntimeConfig(clusters=clusters, projects=projects, default_project="default")
    configure(config=cfg, reset=True)
    project("p1", cpu_quota=max(2, capacity), mem_quota=0, priority=1)


def test_foreach_ordered_skip_errors():
    # 保序 + 错误跳过：只返回成功项，错误索引可追踪。
    _setup_runtime(num_clusters=1, capacity=4)
    result = foreach(
        range(10),
        _square_or_fail,
        mode="ordered",
        on_error="skip",
        retries=1,
        project="p1",
        chunk_size=1,
        include_errors=True,
    )
    assert result.values == [0, 1, 4, 16, 25, 36, 64, 81]
    assert [e.index for e in result.errors] == [3, 7]


def test_foreach_as_completed():
    # as_completed 模式下，不要求与输入顺序一致。
    _setup_runtime(num_clusters=1, capacity=4)
    values = foreach(
        [0, 1, 2, 3, 4],
        _sleep_inverse,
        mode="as_completed",
        on_error="skip",
        project="p1",
        chunk_size=1,
    )
    assert sorted(values) == [0, 1, 2, 3, 4]
    assert values != [0, 1, 2, 3, 4]


def test_multi_cluster_routing_distribution():
    # 验证多个集群都收到任务，证明路由器不是单点倾斜。
    _setup_runtime(num_clusters=2, capacity=2)
    values = foreach(
        list(range(40)),
        _square,
        mode="ordered",
        on_error="skip",
        project="p1",
        chunk_size=1,
    )
    assert values == [x * x for x in range(40)]
    snapshot = get_runtime().gateway.snapshot()
    active_clusters = [name for name, stats in snapshot.items() if stats.submitted > 0]
    assert len(active_clusters) >= 2

from pc import parallel_for

@parallel_for(mode="ordered", on_error="skip", retries=1, project="default")
def calc(nums):
    out = []
    for n in nums:
        out.append(n * n)
    return out

print(calc(list(range(10))))
