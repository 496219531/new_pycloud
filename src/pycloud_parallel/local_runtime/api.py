from __future__ import annotations

"""中文说明：用户主入口 API。

对应你的需求：
1) `parallel_for` 提供“装饰器级改造”，尽量少改原代码。
2) `foreach` 提供显式并行兜底，保证复杂场景可落地。
3) `project` 提供项目级资源隔离入口。
"""

import functools
import inspect
import threading
import warnings
from typing import Callable, Iterable, Optional

from .ast_rewriter import rewrite_function
from .config import ProjectConfig, RuntimeConfig, normalize_runtime_config
from .runtime import Runtime, configure_runtime, get_runtime


def configure(*, config: Optional[RuntimeConfig] = None, reset: bool = True):
    """��置 PyCloud 运行时环境。

    这是统一的配置入口点，仅支持代码内传入 RuntimeConfig。

    Args:
        config: 可选的运行时配置对象
        reset: 是否重置现有运行时（默认为 True）

    Returns:
        Runtime: 配置好的运行时实例
    """
    # 统一入口：仅支持代码配置，未传则使用默认本地配置。
    return configure_runtime(config=config, reset=reset)


def project(name: str, cpu_quota: int) -> str:
    """注册一个项目并设置其资源配额。

    项目用于隔离不同任务的资源使用，确保多个项目可以同时运行而不会相互抢占资源。

    Args:
        name: 项目名称
        cpu_quota: CPU 配额（并发任务数上限）

    Returns:
        str: 返回项目名称

    Example:
        >>> project("data-processing", cpu_quota=4)
        'data-processing'
    """
    runtime = get_runtime()
    runtime.register_project(
        ProjectConfig(
            name=name,
            cpu_quota=max(1, int(cpu_quota)),
        )
    )
    return name


def foreach(
    iterable: Iterable,
    fn: Callable,
    *,
    mode: str = "ordered",
    on_error: Optional[str] = "skip",
    retries: Optional[int] = 0,
    project: Optional[str] = None,
    chunk_size: Optional[int] = None,
    include_errors: bool = False,
    max_workers: Optional[int] = None,
):
    """显式并行执行函数。

    这是最灵活的并行 API，适用于需要显式控制并行的场景。

    Args:
        iterable: 要迭代的数据集合
        fn: 应用于每个元素的函数
        mode: 返回模式，"ordered" 保持原顺序，"as_completed" 按完成顺序返回
        on_error: 错误处理策略，"skip" 跳过错误，"raise" 抛出异常
        retries: 失败重试次数
        project: 项目名称（用于资源隔离）
        chunk_size: 分片大小（None 表示自动计算）
        include_errors: 是否在返回值中包含错误信息
        max_workers: 进程数（None 表示使用现有 runtime，>0 表示创建新的 runtime 并在函数结束时关闭）

    Returns:
        如果 include_errors=False，返回结果列表
        如果 include_errors=True，返回 ForeachResult 对象（包含 values 和 errors）

    Example:
        >>> results = foreach([1, 2, 3], lambda x: x * 2)
        >>> print(results)  # [2, 4, 6]

        >>> # 使用自定义进程数，函数结束后自动关闭进程池
        >>> results = foreach([1, 2, 3], lambda x: x * 2, max_workers=8)
    """
    # 显式并行 API：当自动 AST 改写不适用时，调用方可直接使用。

    # 如果指定了 max_workers，创建临时 runtime 并在函数结束时清理
    should_cleanup = False
    temp_runtime = None
    if max_workers is not None and max_workers > 0:
        # 直接创建新的 Runtime 实例，不影响全局 runtime
        cfg = normalize_runtime_config(RuntimeConfig(max_workers=max_workers))
        temp_runtime = Runtime(cfg)
        should_cleanup = True

    try:
        runtime = temp_runtime if temp_runtime is not None else get_runtime()
        result = runtime.foreach(
            iterable=iterable,
            fn=fn,
            mode=mode,
            on_error=on_error,
            retries=retries,
            project=project,
            chunk_size=chunk_size,
        )

        # 如果使用了临时 runtime，将其错误和指标同步到全局 runtime
        if should_cleanup and temp_runtime is not None:
            global_runtime = get_runtime()
            # 同步错误信息
            temp_errors = temp_runtime.get_last_errors()
            if temp_errors:
                global_runtime._last_errors.value = temp_errors
            # 同步指标
            temp_metrics = temp_runtime.snapshot_metrics()
            with global_runtime._metrics_lock:
                global_runtime.metrics.submitted_jobs += temp_metrics["submitted_jobs"]
                global_runtime.metrics.succeeded_jobs += temp_metrics["succeeded_jobs"]
                global_runtime.metrics.failed_jobs += temp_metrics["failed_jobs"]

        if include_errors:
            return result
        return result.values
    finally:
        # 清理临时创建的 runtime
        if should_cleanup and temp_runtime is not None:
            temp_runtime.shutdown()


def last_errors():
    """获取最后一次 foreach 调用的错误列表。

    Returns:
        List[TaskError]: 错误列表，包含索引、项表示、错误信息等

    Example:
        >>> foreach([1, 2, 3], lambda x: 1/x)  # 会产生错误（x=0）
        >>> errors = last_errors()
        >>> for err in errors:
        ...     print(f"Index {err.index}: {err.error}")
    """
    runtime = get_runtime()
    return runtime.get_last_errors()


def metrics():
    """获取运行时指标统计。

    Returns:
        Dict[str, int]: 包含以下键的字典：
            - submitted_jobs: 已提交的任务数
            - succeeded_jobs: 成功完成的任务数
            - failed_jobs: 失败的任务数

    Example:
        >>> stats = metrics()
        >>> print(f"提交: {stats['submitted_jobs']}, 成功: {stats['succeeded_jobs']}")
    """
    runtime = get_runtime()
    return runtime.snapshot_metrics()


def parallel_for(
    *,
    mode: str = "ordered",
    on_error: str = "skip",
    retries: int = 0,
    project: Optional[str] = None,
    max_workers: Optional[int] = None,
):
    """并行 for 循环装饰器。

    使用 AST 改写技术自动将符合条件的 for 循环转换为并行执行。
    这是最小侵入性的并行化方式，只需在函数上添加装饰器即可。

    支持的循环模式：
    - 循环体最后一条语句必须是 list.append()
    - 不能包含 break、continue、return、yield、raise、try、with 等
    - 不能使用闭包变量（freevars）

    Args:
        mode: 返回模式（"ordered" 或 "as_completed"）
        on_error: 错误处理策略（"skip" 或 "raise"）
        retries: 失败重试次数
        project: 项目名称
        max_workers: 进程数（None 表示使用现有 runtime，>0 表示创建新的 runtime 并在函数结束时关闭）

    Returns:
        Callable: 装饰器函数

    Example:
        >>> @parallel_for(mode="ordered")
        ... def process_items(items):
        ...     results = []
        ...     for item in items:
        ...         results.append(item * 2)
        ...     return results
        >>> process_items([1, 2, 3])  # 会并行执行
        [2, 4, 6]

        >>> # 使用自定义进程数，函数结束后自动关闭进程池
        >>> @parallel_for(max_workers=8)
        ... def process_items(items):
        ...     results = []
        ...     for item in items:
        ...             results.append(item * 2)
        ...     return results
    """
    # 装饰器入口：首次调用时尝试 AST 改写，可改写则并行，不可改写则回退串行。
    def decorator(func: Callable):
        if inspect.iscoroutinefunction(func):
            warnings.warn(
                f"`@parallel_for` does not support async function `{func.__name__}`, fallback to original.",
                RuntimeWarning,
            )
            return func

        lock = threading.Lock()
        compiled_fn = None
        compiled = False
        warned = False

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            nonlocal compiled_fn, compiled, warned
            if not compiled:
                with lock:
                    if not compiled:
                        # 把装饰器参数绑定到 foreach，供 AST 改写后的函数直接调用。
                        parallel_foreach = functools.partial(
                            foreach,
                            mode=mode,
                            on_error=on_error,
                            retries=retries,
                            project=project,
                            max_workers=max_workers,
                        )
                        rewrite = rewrite_function(func, parallel_foreach)
                        if rewrite.function is not None:
                            compiled_fn = rewrite.function
                            setattr(wrapper, "__pycloud_parallelized_loops__", rewrite.rewritten_loops)
                        else:
                            setattr(wrapper, "__pycloud_parallelized_loops__", 0)
                            setattr(wrapper, "__pycloud_rewrite_reason__", rewrite.reason)
                        compiled = True

            if compiled_fn is None:
                if not warned:
                    warnings.warn(
                        f"`@parallel_for` fallback to serial for `{func.__name__}` "
                        f"(reason={getattr(wrapper, '__pycloud_rewrite_reason__', 'unknown')}).",
                        RuntimeWarning,
                    )
                    warned = True
                return func(*args, **kwargs)
            return compiled_fn(*args, **kwargs)

        return wrapper

    return decorator
