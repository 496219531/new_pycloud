from __future__ import annotations

"""中文说明：用户主入口 API。

对应你的需求：
1) `parallel_for` 提供“装饰器级改造”，尽量少改原代码。
2) `foreach` 提供显式并行兜底，保证复杂场景可落地。
"""

import functools
import inspect
import threading
import warnings
from typing import Callable, Iterable, Optional

from .ast_rewriter import rewrite_function
from .runtime import _configure_runtime, _foreach


def configure(*, max_workers: Optional[int] = None, reset: bool = True):
    """配置 PyCloud 运行时环境。

    这是统一的配置入口点，只需要传本地进程数。

    Args:
        max_workers: 本地最大进程数，未传则使用 CPU 核数
        reset: 是否重置现有运行时（默认为 True）

    Returns:
        当前生效的最大进程数
    """
    # 统一入口：仅支持代码配置，未传则使用默认本地配置。
    return _configure_runtime(max_workers=max_workers, reset=reset)

def foreach(
    iterable: Iterable,
    fn: Callable,
    *,
    max_workers: Optional[int] = None,
):
    """显式并行执行函数。

    这是最灵活的并行 API，适用于需要显式控制并行的场景。

    Args:
        iterable: 要迭代的数据集合
        fn: 应用于每个元素的函数
        max_workers: 进程数（None 表示使用现有 runtime，>0 表示创建新的 runtime 并在函数结束时关闭）

    Returns:
        ForeachResult: 包含 values 和 errors

    Example:
        >>> results = foreach([1, 2, 3], lambda x: x * 2)
        >>> print(results.values)  # [2, 4, 6]

        >>> # 使用自定义进程数，函数结束后自动关闭进程池
        >>> results = foreach([1, 2, 3], lambda x: x * 2, max_workers=8)
    """
    # 显式并行 API：当自动 AST 改写不适用时，调用方可直接使用。

    result = _foreach(
        iterable=iterable,
        fn=fn,
        max_workers=max_workers,
    )
    return result


def parallel_for(
    *,
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
        max_workers: 进程数（None 表示使用现有 runtime，>0 表示创建新的 runtime 并在函数结束时关闭）

    Returns:
        Callable: 装饰器函数

    Example:
        >>> @parallel_for()
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
                        def _foreach_values(iterable, fn):
                            return foreach(
                                iterable,
                                fn,
                                max_workers=max_workers,
                            ).values

                        # 把装饰器参数绑定到 foreach，供 AST 改写后的函数直接调用。
                        rewrite = rewrite_function(func, _foreach_values)
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
