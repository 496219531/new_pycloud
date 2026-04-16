"""演示 max_workers 参数的自动进程池管理功能。

这个示例展示了如何使用 max_workers 参数来创���临时的进程池，
该进程池会在函数执行完成后自动关闭。
"""

from pycloud_parallel.local import foreach, parallel_for


def square(x):
    """计算平方"""
    return x * x


def example_basic_foreach():
    """基本的 foreach 用法，使用 max_workers 参数。"""
    print("示例 1: 使用 max_workers=4 的 foreach")
    print("=" * 50)

    # 使用 4 个进程执行，函数结束后进程池自动关闭
    result = foreach(
        [1, 2, 3, 4, 5],
        square,
        max_workers=4,
    )

    print(f"结果: {result}")
    print("✓ 进程池已自动关闭\n")


def example_parallel_for_decorator():
    """使用 parallel_for 装饰器与 max_workers 参数。"""
    print("示例 2: 使用 max_workers=2 的 parallel_for 装饰器")
    print("=" * 50)

    @parallel_for(max_workers=2)
    def process_items(items):
        """处理项目列表"""
        results = []
        for item in items:
            results.append(item * 3)
        return results

    # 使用 2 个进程执行，函数结束后进程池自动关闭
    result = process_items([1, 2, 3, 4, 5])

    print(f"结果: {result}")
    print("✓ 进程池已自动关闭\n")


def example_mixed_usage():
    """混合使用全局 runtime 和临时 runtime。"""
    print("示例 3: 混合使用全局 runtime 和临时 runtime")
    print("=" * 50)

    from pycloud_parallel.local import configure

    # 配置全局 runtime（2 个进程）
    configure(max_workers=2, reset=True)
    print("✓ 全局 runtime 已配置（2 个进程）")

    # 使用全局 runtime
    result1 = foreach([1, 2], square)
    print(f"使用全局 runtime: {result1}")

    # 使用临时 runtime（4 个进程）
    result2 = foreach([3, 4, 5, 6], square, max_workers=4)
    print(f"使用临时 runtime（4 个进程）: {result2}")
    print("✓ 临时 runtime 已关闭")

    # 再次使用全局 runtime
    result3 = foreach([7, 8], square)
    print(f"再次使用全局 runtime: {result3}")

    print("✓ 全局 runtime 仍然可用\n")


def example_multiple_calls():
    """多次调用使用不同的 max_workers。"""
    print("示例 4: 多次调用使用不同的 max_workers")
    print("=" * 50)

    # 第一次调用：2 个进程
    result1 = foreach([1, 2, 3], square, max_workers=2)
    print(f"调用 1（2 个进程）: {result1}")
    print("✓ 进程池已关闭")

    # 第二次调用：4 个进程
    result2 = foreach([4, 5, 6], square, max_workers=4)
    print(f"调用 2（4 个进程）: {result2}")
    print("✓ 进程池已关闭")

    # 第三次调用：2 个进程
    result3 = foreach([7, 8, 9], square, max_workers=2)
    print(f"调用 3（2 个进程）: {result3}")
    print("✓ 进程池已关闭\n")


def example_without_max_workers():
    """不指定 max_workers 时使用全局 runtime。"""
    print("示例 5: 不指定 max_workers（使用全局 runtime）")
    print("=" * 50)

    from pycloud_parallel.local import configure

    # 配置全局 runtime
    configure(max_workers=3, reset=True)
    print("✓ 全局 runtime 已配置（3 个进程）")

    # 不指定 max_workers，使用全局 runtime
    result = foreach([1, 2, 3, 4, 5], square)
    print(f"结果: {result}")
    print("✓ 使用全局 runtime（不会自动关闭）\n")


if __name__ == "__main__":
    print("\n" + "=" * 50)
    print("max_workers 参数自动进程池管理演示")
    print("=" * 50 + "\n")

    example_basic_foreach()
    example_parallel_for_decorator()
    example_mixed_usage()
    example_multiple_calls()
    example_without_max_workers()

    print("=" * 50)
    print("所有示例运行完成！")
    print("=" * 50)
