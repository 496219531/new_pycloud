"""测试 max_workers 参数的自动进程池管理功能。"""

import os
import time

from pycloud_parallel.local import configure, foreach, parallel_for


def _square(x):
    time.sleep(0.01)
    return x * x


def test_foreach_with_max_workers():
    """测试 foreach 使用 max_workers 参数时自动创建和关闭进程池。"""
    # 不使用全局 runtime，直接指定 max_workers
    result = foreach(
        [1, 2, 3, 4],
        _square,
        max_workers=2,
    )
    assert result.values == [1, 4, 9, 16]


def test_foreach_without_max_workers():
    """测试 foreach 不指定 max_workers 时使用全局 runtime。"""
    # 先配置全局 runtime
    configure(max_workers=2)

    # 不指定 max_workers，应该使用全局 runtime
    result = foreach(
        [1, 2, 3, 4],
        _square,
    )
    assert result.values == [1, 4, 9, 16]


def test_parallel_for_with_max_workers():
    """测试 parallel_for 使用 max_workers 参数。"""

    @parallel_for(max_workers=2)
    def process_items(items):
        results = []
        for item in items:
            results.append(item * 2)
        return results

    result = process_items([1, 2, 3, 4])
    assert result == [2, 4, 6, 8]


def test_max_workers_cleanup():
    """测试 max_workers 指定的进程池在函数结束后被清理。"""
    # 多次调用使用不同的 max_workers，应该不会冲突
    result1 = foreach([1, 2], _square, max_workers=2)
    assert result1.values == [1, 4]

    result2 = foreach([3, 4], _square, max_workers=4)
    assert result2.values == [9, 16]

    result3 = foreach([5, 6], _square, max_workers=2)
    assert result3.values == [25, 36]


def test_foreach_mixed_usage():
    """测试混合使用全局 runtime 和临时 runtime。"""
    # 配置全局 runtime
    configure(max_workers=2)

    # 使用全局 runtime
    result1 = foreach([1, 2], _square)
    assert result1.values == [1, 4]

    # 使用临时 runtime
    result2 = foreach([3, 4], _square, max_workers=4)
    assert result2.values == [9, 16]

    # 再次使用全局 runtime
    result3 = foreach([5, 6], _square)
    assert result3.values == [25, 36]


def test_max_workers_zero_or_none():
    """测试 max_workers 为 0、None 或负数时的行为。"""
    configure(max_workers=2)

    # max_workers=None 应该使用全局 runtime
    result1 = foreach([1, 2], _square, max_workers=None)
    assert result1.values == [1, 4]

    # max_workers=0 应该使用全局 runtime
    result2 = foreach([3, 4], _square, max_workers=0)
    assert result2.values == [9, 16]

    # 不指定 max_workers 应该使用全局 runtime
    result3 = foreach([5, 6], _square)
    assert result3.values == [25, 36]


if __name__ == "__main__":
    test_foreach_with_max_workers()
    print("✓ test_foreach_with_max_workers passed")

    test_foreach_without_max_workers()
    print("✓ test_foreach_without_max_workers passed")

    test_parallel_for_with_max_workers()
    print("✓ test_parallel_for_with_max_workers passed")

    test_max_workers_cleanup()
    print("✓ test_max_workers_cleanup passed")

    test_foreach_mixed_usage()
    print("✓ test_foreach_mixed_usage passed")

    test_max_workers_zero_or_none()
    print("✓ test_max_workers_zero_or_none passed")

    print("\nAll tests passed! ✓")
