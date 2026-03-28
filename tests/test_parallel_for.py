"""中文说明：验证装饰器 AST 改写能力与串行回退策略。"""

import warnings

from pc import ClusterConfig, ProjectConfig, RuntimeConfig, configure, parallel_for, project


@parallel_for(mode="ordered", on_error="skip", retries=0, project="p1")
def _decorated_square(nums):
    out = []
    for n in nums:
        out.append(n * n)
    return out


@parallel_for(mode="ordered", on_error="skip", retries=0, project="p1")
def _decorated_cumulative(nums):
    out = []
    total = 0
    for n in nums:
        total += n
        out.append(total)
    return out


def _setup_runtime():
    cfg = RuntimeConfig(
        clusters=[ClusterConfig(name="local", address="local", weight=1.0, capacity=4, use_ray=False)],
        projects={"default": ProjectConfig(name="default", cpu_quota=4, mem_quota=0, priority=1)},
        default_project="default",
    )
    configure(config=cfg, reset=True)
    project("p1", cpu_quota=4, mem_quota=0, priority=1)


def test_parallel_for_rewrite_success():
    # 简单 append 循环可被改写并行。
    _setup_runtime()
    values = _decorated_square(list(range(8)))
    assert values == [x * x for x in range(8)]
    assert getattr(_decorated_square, "__pycloud_parallelized_loops__", 0) == 1


def test_parallel_for_fallback_for_unsupported_loop():
    # 累加依赖跨迭代状态，属于不安全改写场景，应自动回退串行。
    _setup_runtime()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        values = _decorated_cumulative([1, 2, 3, 4])
    assert values == [1, 3, 6, 10]
    assert getattr(_decorated_cumulative, "__pycloud_parallelized_loops__", 0) == 0
