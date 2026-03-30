"""演示临时 runtime 与全局 runtime 的错误/指标同步功能。"""

from pycloud_parallel import foreach, last_errors, metrics


def divide(x):
    """除法函数，会除以零导致错误。"""
    if x == 0:
        raise ValueError("Cannot divide by zero")
    return 10 / x


def square(x):
    """平方函数。"""
    return x * x


def demo_error_sync():
    """演示错误信息同步。"""
    print("=" * 60)
    print("演示 1: 临时 runtime 的错误信息同步到全局 runtime")
    print("=" * 60)

    # 使用临时 runtime 执行会失败的任务
    print("\n执行: foreach([1, 2, 0, 4], divide, max_workers=2)")
    result = foreach([1, 2, 0, 4], divide, max_workers=2, on_error="skip")

    print(f"结果: {result}")
    print("注意: 索引 2 的项（值为 0）被跳过了")

    # 使用 last_errors() 获取错误
    errors = last_errors()
    print(f"\n通过 last_errors() 获取错误信息:")
    print(f"  错误数量: {len(errors)}")
    for err in errors:
        print(f"  - 索引 {err.index}: {err.error}")

    print("\n✓ 即使使用了临时 runtime (max_workers=2)")
    print("✓ 仍然可以通过 last_errors() 获取错误信息\n")


def demo_metrics_sync():
    """演示指标同步。"""
    print("=" * 60)
    print("演示 2: 临时 runtime 的指标同步到全局 runtime")
    print("=" * 60)

    from pycloud_parallel import configure, RuntimeConfig

    # 重置全局 runtime
    configure(config=RuntimeConfig(max_workers=2), reset=True)
    print("\n已重置全局 runtime")

    # 第一次调用：临时 runtime
    print("\n执行: foreach([1, 2, 3], square, max_workers=2)")
    result1 = foreach([1, 2, 3], square, max_workers=2)
    print(f"结果: {result1}")

    stats1 = metrics()
    print(f"\n第一次调用后的指标:")
    print(f"  已提交任务: {stats1['submitted_jobs']}")
    print(f"  成功任务: {stats1['succeeded_jobs']}")
    print(f"  失败任务: {stats1['failed_jobs']}")

    # 第二次调用：另一个临时 runtime
    print("\n执行: foreach([4, 5, 6], square, max_workers=3)")
    result2 = foreach([4, 5, 6], square, max_workers=3)
    print(f"结果: {result2}")

    stats2 = metrics()
    print(f"\n第二次调用后的指标:")
    print(f"  已提交任务: {stats2['submitted_jobs']}")
    print(f"  成功任务: {stats2['succeeded_jobs']}")
    print(f"  失败任务: {stats2['failed_jobs']}")

    print("\n✓ 指标累加了！")
    print("✓ 即使使用不同的临时 runtime，指标也会同步到全局 runtime\n")


def demo_mixed_runtime():
    """演示混合使用全局和临时 runtime。"""
    print("=" * 60)
    print("演示 3: 混合使用全局 runtime 和临时 runtime")
    print("=" * 60)

    from pycloud_parallel import configure, RuntimeConfig

    # 配置全局 runtime
    configure(config=RuntimeConfig(max_workers=2))
    print("\n已配置全局 runtime (max_workers=2)")

    # 使用全局 runtime
    print("\n[全局 runtime] 执行: foreach([1, 2], square)")
    result1 = foreach([1, 2], square)
    print(f"结果: {result1}")

    stats1 = metrics()
    print(f"指标: submitted={stats1['submitted_jobs']}, succeeded={stats1['succeeded_jobs']}")

    # 使用临时 runtime
    print("\n[临时 runtime] 执行: foreach([3, 4], square, max_workers=4)")
    result2 = foreach([3, 4], square, max_workers=4)
    print(f"结果: {result2}")

    stats2 = metrics()
    print(f"指标: submitted={stats2['submitted_jobs']}, succeeded={stats2['succeeded_jobs']}")
    print("注意: 指标累加了（临时 runtime 的指标已同步）")

    # 再次使用全局 runtime
    print("\n[全局 runtime] 执行: foreach([5, 6], square)")
    result3 = foreach([5, 6], square)
    print(f"结果: {result3}")

    stats3 = metrics()
    print(f"指标: submitted={stats3['submitted_jobs']}, succeeded={stats3['succeeded_jobs']}")

    print("\n✓ 全局 runtime 和临时 runtime 的指标都正确同步了\n")


def demo_error_accumulation():
    """演示错误累加。"""
    print("=" * 60)
    print("演示 4: 多次调用的错误信息")
    print("=" * 60)

    from pycloud_parallel import configure, RuntimeConfig

    # 重置
    configure(config=RuntimeConfig(max_workers=2), reset=True)

    # 第一次调用
    print("\n执行: foreach([1, 0, 2], divide, max_workers=2)")
    foreach([1, 0, 2], divide, max_workers=2, on_error="skip")

    errors1 = last_errors()
    print(f"错误数量: {len(errors1)}")

    # 第二次调用
    print("\n执行: foreach([3, 0, 4], divide, max_workers=2)")
    foreach([3, 0, 4], divide, max_workers=2, on_error="skip")

    errors2 = last_errors()
    print(f"错误数量: {len(errors2)}")
    print(f"最新的错误索引: {errors2[0].index}")

    print("\n✓ last_errors() 返回的是最后一次调用的错误")
    print("✓ 旧的错误信息会被新的覆盖\n")


if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("临时 runtime 与全局 runtime 的错误/指标同步演示")
    print("=" * 60 + "\n")

    demo_error_sync()
    demo_metrics_sync()
    demo_mixed_runtime()
    demo_error_accumulation()

    print("=" * 60)
    print("所有演示运行完成！")
    print("=" * 60)
