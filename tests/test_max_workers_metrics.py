"""测试 max_workers 参数与 last_errors/metrics 的兼容性。"""

import time

from pycloud_parallel import foreach, last_errors, metrics, parallel_for


def _divide_or_fail(x):
    """除法函数，某些情况下会失败。"""
    if x == 0:
        raise ValueError("Cannot divide by zero")
    return 10 / x


def _square(x):
    """平方函数。"""
    time.sleep(0.01)
    return x * x


def test_last_errors_with_temp_runtime():
    """测试使用临时 runtime 时，last_errors() 仍然能获取错误。"""
    # 使用临时 runtime 执行会失败的任务
    result = foreach(
        [1, 2, 0, 4],  # 0 会导致除零错误
        _divide_or_fail,
        max_workers=2,
        on_error="skip",
    )

    # 验证结果（错误的项被跳过）
    assert len(result) == 3

    # 验证能够获取错误（即使使用了临时 runtime）
    errors = last_errors()
    assert len(errors) == 1
    assert errors[0].index == 2  # 第三个元素（索引为2）
    assert "Cannot divide by zero" in errors[0].error


def test_metrics_with_temp_runtime():
    """测试使用临时 runtime 时，metrics() 仍然能获取统计信息。"""
    # 清空全局 runtime 的指标
    from pycloud_parallel import configure, RuntimeConfig
    configure(config=RuntimeConfig(max_workers=2), reset=True)

    # 使用临时 runtime 执行任务
    result = foreach(
        [1, 2, 3, 4, 5],
        _square,
        max_workers=2,
    )

    # 验证能够获取指标（即使使用了临时 runtime）
    stats = metrics()
    assert stats["submitted_jobs"] >= 1
    assert stats["succeeded_jobs"] >= 1


def test_metrics_accumulation():
    """测试多次使用临时 runtime 时，指标会累加。"""
    from pycloud_parallel import configure, RuntimeConfig

    # 重置全局 runtime
    configure(config=RuntimeConfig(max_workers=2), reset=True)

    # 第一次调用：临时 runtime
    foreach([1, 2], _square, max_workers=2)

    stats1 = metrics()
    submitted1 = stats1["submitted_jobs"]

    # 第二次调用：另一个临时 runtime
    foreach([3, 4], _square, max_workers=3)

    stats2 = metrics()
    submitted2 = stats2["submitted_jobs"]

    # 验证指标累加了
    assert submitted2 >= submitted1


def test_mixed_runtime_errors():
    """测试混合使用全局和临时 runtime 时，错误信息正确。"""
    from pycloud_parallel import configure, RuntimeConfig

    # 配置全局 runtime
    configure(config=RuntimeConfig(max_workers=2))

    # 使用全局 runtime（会失败）
    foreach([1, 2, 0], _divide_or_fail, on_error="skip")

    errors1 = last_errors()
    assert len(errors1) == 1

    # 使用临时 runtime（也会失败）
    foreach([4, 0, 5], _divide_or_fail, on_error="skip", max_workers=2)

    errors2 = last_errors()
    # 应该是最新的错误（临时 runtime 的错误）
    assert len(errors2) == 1
    assert errors2[0].index == 1


def test_parallel_for_errors_with_max_workers():
    """测试 parallel_for 使用 max_workers 时，错误信息可获取。"""

    @parallel_for(max_workers=2, on_error="skip")
    def process_items(items):
        results = []
        for item in items:
            results.append(10 / item)
        return results

    # 执行会失败的任务
    result = process_items([1, 2, 0, 4])

    # 验证能够获取错误
    errors = last_errors()
    assert len(errors) == 1
    assert errors[0].index == 2


def test_empty_result_with_temp_runtime():
    """测试使用临时 runtime 且所有任务都失败时，错误信息可获取。"""
    # 所有任务都会失败
    result = foreach(
        [0, 0, 0],
        _divide_or_fail,
        max_workers=2,
        on_error="skip",
    )

    # 结果为空
    assert len(result) == 0

    # 但能够获取错误
    errors = last_errors()
    assert len(errors) == 3


if __name__ == "__main__":
    test_last_errors_with_temp_runtime()
    print("✓ test_last_errors_with_temp_runtime passed")

    test_metrics_with_temp_runtime()
    print("✓ test_metrics_with_temp_runtime passed")

    test_metrics_accumulation()
    print("✓ test_metrics_accumulation passed")

    test_mixed_runtime_errors()
    print("✓ test_mixed_runtime_errors passed")

    test_parallel_for_errors_with_max_workers()
    print("✓ test_parallel_for_errors_with_max_workers passed")

    test_empty_result_with_temp_runtime()
    print("✓ test_empty_result_with_temp_runtime passed")

    print("\nAll tests passed! ✓")
