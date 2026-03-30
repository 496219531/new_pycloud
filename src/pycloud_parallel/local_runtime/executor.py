from __future__ import annotations

"""中文说明：并行执行核心。

职责：
1) 迭代数据分片（chunk）并批量提交，降低调度开销。
2) 支持 ordered/as_completed 两种返回语义。
3) 支持任务失败跳过与重试。
"""

from concurrent.futures import FIRST_COMPLETED, wait
from typing import Dict, Iterable, Iterator, List, Optional, Sequence, Tuple, Union

import cloudpickle

from .project_manager import ProjectManager
from .runners import ProcessClusterRunner
from .types import ForeachResult, TaskError, UserFunctionError


def _auto_chunk_size(total_items: Optional[int], width: int) -> int:
    """自动计算最优分片大小。

    根据总项数和并行宽度，计算合适的分片大小以平衡吞吐量和调度开销。

    Args:
        total_items: 总项数（如果未知则为 None）
        width: 并行宽度（可用的工作线程数）

    Returns:
        int: 计算出的分片大小
    """
    # 自适应分片：以”并行宽度 * 常数批次数”为目标，兼顾吞吐与开销。
    width = max(1, width)
    if total_items is None:
        return max(1, min(64, 4 * width))
    target_batches = width * 8
    return max(1, min(256, (total_items + target_batches - 1) // target_batches))


def _chunked(indexed_iter: Iterable[Tuple[int, object]], chunk_size: int) -> Iterator[List[Tuple[int, object]]]:
    """将迭代器分块。

    Args:
        indexed_iter: 带索引的迭代器
        chunk_size: 每块的大小

    Yields:
        List[Tuple[int, object]]: 分块后的列表
    """
    chunk: List[Tuple[int, object]] = []
    for item in indexed_iter:
        chunk.append(item)
        if len(chunk) >= chunk_size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def run_foreach(
    iterable: Union[Sequence[object], Iterable[object]],
    fn,
    runner: ProcessClusterRunner,
    projects: ProjectManager,
    *,
    mode: str,
    on_error: str,
    retries: int,
    project: str,
    chunk_size: Optional[int],
) -> ForeachResult:
    """并行执行 foreach 操作的核心实现。

    负责任务分片、提交、错误处理和结果收集。

    Args:
        iterable: 可迭代对象
        fn: 要执行的函数
        runner: 本地进程池执行器
        projects: 项目管理器
        mode: 返回模式
        on_error: 错误处理策略
        retries: 重试次数
        project: 项目名称
        chunk_size: 分片大小

    Returns:
        ForeachResult: 包含结果和错误的对象

    Raises:
        ValueError: 当参数无效时
        RuntimeError: 当函数序列化失败时
    """
    # 参数校验：在入口处尽早失败，避免隐藏行为。
    if mode not in ("ordered", "as_completed"):
        raise ValueError("mode must be `ordered` or `as_completed`")
    if on_error not in ("skip", "raise"):
        raise ValueError("on_error must be `skip` or `raise`")
    if retries < 0:
        raise ValueError("retries must be >= 0")

    projects.ensure(project, default_cpu=1)

    try:
        serialized_fn = cloudpickle.dumps(fn)
    except Exception as exc:
        raise RuntimeError(f"failed to serialize function for parallel execution: {exc}") from exc

    total_items = None
    try:
        total_items = len(iterable)  # type: ignore[arg-type]
    except Exception:
        total_items = None

    actual_chunk_size = chunk_size or _auto_chunk_size(total_items=total_items, width=runner.capacity)
    indexed_chunks = _chunked(enumerate(iterable), chunk_size=actual_chunk_size)

    max_pending = max(1, runner.capacity * 2)
    pending: Dict[object, List[Tuple[int, object]]] = {}
    all_errors: List[TaskError] = []
    ordered_values: Dict[int, object] = {}
    completed_values: List[object] = []
    input_exhausted = False

    def _submit_chunk(
        indexed_items: List[Tuple[int, object]],
    ) -> None:
        """提交一个分片到集群执行。

        Args:
            indexed_items: 带索引的项目列表
        """
        # 项目级信号量：限制单项目并发，避免多个项目相互抢占。
        projects.acquire(project)
        future = runner.submit_chunk(
            serialized_fn=serialized_fn,
            indexed_items=indexed_items,
            retries=retries,
            on_error=on_error,
        )
        future.add_done_callback(lambda _f, _project=project: projects.release(_project))
        pending[future] = indexed_items

    while len(pending) < max_pending and not input_exhausted:
        try:
            chunk = next(indexed_chunks)
        except StopIteration:
            input_exhausted = True
            break
        _submit_chunk(chunk)

    while pending:
        done, _ = wait(list(pending.keys()), return_when=FIRST_COMPLETED)
        for fut in done:
            indexed_items = pending.pop(fut)
            try:
                values, errors = fut.result()
            except UserFunctionError as exc:
                if on_error == "raise":
                    raise
                all_errors.append(
                    TaskError(
                        index=exc.index,
                        item_repr=exc.item_repr,
                        error=exc.cause,
                        attempts=retries + 1,
                    )
                )
            except Exception as exc:
                # 本地模式下不存在跨集群 failover，按 on_error 策略处理。
                if on_error == "raise":
                    raise RuntimeError(f"local execution failed: {exc}") from exc
                for idx, item in indexed_items:
                    all_errors.append(
                        TaskError(
                            index=idx,
                            item_repr=repr(item),
                            error=f"local execution failure: {repr(exc)}",
                            attempts=1,
                        )
                    )
            else:
                all_errors.extend(errors)
                if mode == "ordered":
                    for idx, value in values:
                        ordered_values[idx] = value
                else:
                    completed_values.extend([value for _, value in values])

        while len(pending) < max_pending and not input_exhausted:
            try:
                chunk = next(indexed_chunks)
            except StopIteration:
                input_exhausted = True
                break
            _submit_chunk(chunk)

    if mode == "ordered":
        values = [ordered_values[idx] for idx in sorted(ordered_values.keys())]
    else:
        values = completed_values
    return ForeachResult(values=values, errors=all_errors)
