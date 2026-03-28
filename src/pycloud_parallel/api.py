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
from .config import ProjectConfig, RuntimeConfig
from .runtime import configure_runtime, get_runtime


def configure(*, config: Optional[RuntimeConfig] = None, config_path: Optional[str] = None, reset: bool = True):
    # 统一入口：支持代码内传配置或从 pycloud.yaml/环境变量加载。
    return configure_runtime(config=config, config_path=config_path, reset=reset)


def project(name: str, cpu_quota: int, mem_quota: int = 0, priority: int = 1) -> str:
    runtime = get_runtime()
    runtime.register_project(
        ProjectConfig(
            name=name,
            cpu_quota=max(1, int(cpu_quota)),
            mem_quota=max(0, int(mem_quota)),
            priority=max(1, int(priority)),
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
    cluster_policy: str = "weighted_least_load",
    chunk_size: Optional[int] = None,
    include_errors: bool = False,
):
    # 显式并行 API：当自动 AST 改写不适用时，调用方可直接使用。
    runtime = get_runtime()
    result = runtime.foreach(
        iterable=iterable,
        fn=fn,
        mode=mode,
        on_error=on_error,
        retries=retries,
        project=project,
        cluster_policy=cluster_policy,
        chunk_size=chunk_size,
    )
    if include_errors:
        return result
    return result.values


def last_errors():
    runtime = get_runtime()
    return runtime.get_last_errors()


def metrics():
    runtime = get_runtime()
    return runtime.snapshot_metrics()


def parallel_for(
    *,
    mode: str = "ordered",
    on_error: str = "skip",
    retries: int = 0,
    project: Optional[str] = None,
    cluster_policy: str = "weighted_least_load",
):
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
                            cluster_policy=cluster_policy,
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
