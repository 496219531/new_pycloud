"""中文说明：验证 foreach 的核心语义。

覆盖点：保序输出、as_completed 输出、失败跳过、本地执行统计。
"""

import time

from pycloud_parallel import ProjectConfig, RuntimeConfig, configure, foreach, get_runtime, project


def _square_or_fail(x):
    if x in (3, 7):
        raise ValueError("boom")
    return x * x


def _sleep_inverse(x):
    time.sleep((5 - x) * 0.02)
    return x


def _square(x):
    return x * x


def _setup_runtime(max_workers=4):
    projects = {"default": ProjectConfig(name="default", cpu_quota=max(2, max_workers))}
    cfg = RuntimeConfig(max_workers=max_workers, projects=projects, default_project="default")
    configure(config=cfg, reset=True)
    project("p1", cpu_quota=max(2, max_workers))


def test_foreach_ordered_skip_errors():
    # 保序 + 错误跳过：只返回成功项，错误索引可追踪。
    _setup_runtime(max_workers=4)
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
    _setup_runtime(max_workers=4)
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


def test_local_runtime_metrics():
    # 本地模式下，任务执行后应记录提交与成功指标。
    _setup_runtime(max_workers=4)
    values = foreach(
        list(range(40)),
        _square,
        mode="ordered",
        on_error="skip",
        project="p1",
        chunk_size=1,
    )
    assert values == [x * x for x in range(40)]
    m = get_runtime().snapshot_metrics()
    assert m["submitted_jobs"] >= 1
    assert m["succeeded_jobs"] >= 1
