from __future__ import annotations

"""中文说明：并行执行核心。

职责：
1) 迭代数据分片（chunk）并批量提交，降低调度开销。
2) 支持 ordered/as_completed 两种返回语义。
3) 支持任务失败跳过、重试与集群失败自动切换（failover）。
"""

from concurrent.futures import FIRST_COMPLETED, wait
from typing import Dict, Iterable, Iterator, List, Optional, Sequence, Tuple, Union

import cloudpickle

from .gateway import ClusterGateway
from .project_manager import ProjectManager
from .types import ChunkMeta, ForeachResult, TaskError, UserFunctionError


def _auto_chunk_size(total_items: Optional[int], width: int) -> int:
    # 自适应分片：以“并行宽度 * 常数批次数”为目标，兼顾吞吐与开销。
    width = max(1, width)
    if total_items is None:
        return max(1, min(64, 4 * width))
    target_batches = width * 8
    return max(1, min(256, (total_items + target_batches - 1) // target_batches))


def _chunked(indexed_iter: Iterable[Tuple[int, object]], chunk_size: int) -> Iterator[List[Tuple[int, object]]]:
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
    gateway: ClusterGateway,
    projects: ProjectManager,
    *,
    mode: str,
    on_error: str,
    retries: int,
    project: str,
    cluster_policy: str,
    chunk_size: Optional[int],
) -> ForeachResult:
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

    actual_chunk_size = chunk_size or _auto_chunk_size(total_items=total_items, width=gateway.total_parallelism())
    indexed_chunks = _chunked(enumerate(iterable), chunk_size=actual_chunk_size)

    max_pending = max(1, gateway.total_parallelism() * 2)
    max_failovers = 2
    pending: Dict[object, ChunkMeta] = {}
    all_errors: List[TaskError] = []
    ordered_values: Dict[int, object] = {}
    completed_values: List[object] = []
    input_exhausted = False

    def _submit_chunk(
        indexed_items: List[Tuple[int, object]],
        *,
        excluded: Optional[set] = None,
        failovers: int = 0,
    ) -> None:
        # 项目级信号量：限制单项目并发，避免多个项目相互抢占。
        projects.acquire(project)
        future, cluster = gateway.submit(
            serialized_fn=serialized_fn,
            indexed_items=indexed_items,
            retries=retries,
            on_error=on_error,
            policy=cluster_policy,
            exclude=excluded,
        )
        future.add_done_callback(lambda _f, _project=project: projects.release(_project))
        pending[future] = ChunkMeta(
            indexed_items=indexed_items,
            cluster=cluster,
            failovers=failovers,
            excluded_clusters=set(excluded or set()),
        )

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
            meta = pending.pop(fut)
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
                        cluster=exc.cluster,
                        attempts=retries + 1,
                    )
                )
            except Exception as exc:
                # 集群级异常按块重投到其他集群，提升可用性。
                gateway.mark_unhealthy(meta.cluster)
                excluded = set(meta.excluded_clusters or set())
                excluded.add(meta.cluster)
                if meta.failovers < max_failovers:
                    try:
                        _submit_chunk(meta.indexed_items, excluded=excluded, failovers=meta.failovers + 1)
                        continue
                    except Exception:
                        pass
                if on_error == "raise":
                    raise RuntimeError(
                        f"cluster execution failed after failover attempts on {meta.cluster}: {exc}"
                    ) from exc
                for idx, item in meta.indexed_items:
                    all_errors.append(
                        TaskError(
                            index=idx,
                            item_repr=repr(item),
                            error=f"cluster execution failure: {repr(exc)}",
                            cluster=meta.cluster,
                            attempts=meta.failovers + 1,
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
