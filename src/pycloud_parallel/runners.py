from __future__ import annotations

"""中文说明：底层执行 Runner。

默认使用 ProcessPool 执行 CPU 密集任务；当集群配置允许时，
子进程里会尝试初始化 Ray，以支持远端集群任务执行。
"""

import os
import multiprocessing as mp
from concurrent.futures import Future, ProcessPoolExecutor
from typing import Dict, List, Tuple

import cloudpickle

from .config import ClusterConfig
from .types import TaskError, UserFunctionError

_CHILD_CLUSTER_NAME = "local"
_CHILD_RAY_ENABLED = False
_CHILD_RAY = None
_CHILD_RAY_TASK = None


def _ray_eval(serialized_fn: bytes, index: int, item):
    fn = cloudpickle.loads(serialized_fn)
    return index, fn(item)


def _runner_initializer(cluster_name: str, address: str, use_ray: bool) -> None:
    # 每个子进程启动时初始化一次上下文，减少每任务开销。
    global _CHILD_CLUSTER_NAME, _CHILD_RAY_ENABLED, _CHILD_RAY, _CHILD_RAY_TASK
    _CHILD_CLUSTER_NAME = cluster_name
    _CHILD_RAY_ENABLED = False
    _CHILD_RAY = None
    _CHILD_RAY_TASK = None

    if not use_ray:
        return

    try:
        import ray  # type: ignore
    except Exception:
        return

    try:
        if not ray.is_initialized():
            if address and address not in ("", "local", "auto"):
                ray.init(address=address, ignore_reinit_error=True, namespace=f"pycloud-{cluster_name}")
            else:
                ray.init(ignore_reinit_error=True)
        _CHILD_RAY_TASK = ray.remote(_ray_eval)
        _CHILD_RAY = ray
        _CHILD_RAY_ENABLED = True
    except Exception:
        _CHILD_RAY_ENABLED = False
        _CHILD_RAY = None
        _CHILD_RAY_TASK = None


def _execute_chunk_local(
    serialized_fn: bytes,
    indexed_items: List[Tuple[int, object]],
    retries: int,
    on_error: str,
) -> Tuple[List[Tuple[int, object]], List[TaskError]]:
    # 本地执行路径：逐项执行 + 按策略重试/跳过/抛错。
    fn = cloudpickle.loads(serialized_fn)
    values: List[Tuple[int, object]] = []
    errors: List[TaskError] = []

    for index, item in indexed_items:
        attempt = 0
        while True:
            try:
                values.append((index, fn(item)))
                break
            except Exception as exc:
                if attempt < retries:
                    attempt += 1
                    continue
                if on_error == "raise":
                    raise UserFunctionError(
                        index=index,
                        item_repr=repr(item),
                        cluster=_CHILD_CLUSTER_NAME,
                        cause=repr(exc),
                    ) from exc
                errors.append(
                    TaskError(
                        index=index,
                        item_repr=repr(item),
                        error=repr(exc),
                        cluster=_CHILD_CLUSTER_NAME,
                        attempts=attempt + 1,
                    )
                )
                break

    return values, errors


def _execute_chunk_ray(
    serialized_fn: bytes,
    indexed_items: List[Tuple[int, object]],
    retries: int,
    on_error: str,
) -> Tuple[List[Tuple[int, object]], List[TaskError]]:
    # Ray 执行路径：并行提交 object refs，按完成顺序回收。
    if not _CHILD_RAY_ENABLED or _CHILD_RAY is None or _CHILD_RAY_TASK is None:
        return _execute_chunk_local(serialized_fn, indexed_items, retries, on_error)

    values: List[Tuple[int, object]] = []
    errors: List[TaskError] = []
    pending: Dict[object, Tuple[int, object, int]] = {}

    for index, item in indexed_items:
        ref = _CHILD_RAY_TASK.remote(serialized_fn, index, item)
        pending[ref] = (index, item, 0)

    while pending:
        ready, _ = _CHILD_RAY.wait(list(pending.keys()), num_returns=1)
        ref = ready[0]
        index, item, attempt = pending.pop(ref)
        try:
            out_index, value = _CHILD_RAY.get(ref)
        except Exception as exc:
            if attempt < retries:
                retry_ref = _CHILD_RAY_TASK.remote(serialized_fn, index, item)
                pending[retry_ref] = (index, item, attempt + 1)
                continue
            if on_error == "raise":
                raise UserFunctionError(
                    index=index,
                    item_repr=repr(item),
                    cluster=_CHILD_CLUSTER_NAME,
                    cause=repr(exc),
                ) from exc
            errors.append(
                TaskError(
                    index=index,
                    item_repr=repr(item),
                    error=repr(exc),
                    cluster=_CHILD_CLUSTER_NAME,
                    attempts=attempt + 1,
                )
            )
        else:
            values.append((out_index, value))

    return values, errors


def _execute_chunk(
    serialized_fn: bytes,
    indexed_items: List[Tuple[int, object]],
    retries: int,
    on_error: str,
) -> Tuple[List[Tuple[int, object]], List[TaskError]]:
    if _CHILD_RAY_ENABLED:
        return _execute_chunk_ray(serialized_fn, indexed_items, retries, on_error)
    return _execute_chunk_local(serialized_fn, indexed_items, retries, on_error)


class ProcessClusterRunner:
    def __init__(self, config: ClusterConfig) -> None:
        self._config = config
        workers = max(1, min(config.capacity, os.cpu_count() or config.capacity))
        use_ray = config.use_ray and config.address not in ("", "local")
        # Unix 上优先 fork，减少 spawn 在交互环境中的兼容问题。
        mp_context = None
        if os.name != "nt":
            try:
                mp_context = mp.get_context("fork")
            except ValueError:
                mp_context = None
        self._pool = ProcessPoolExecutor(
            max_workers=workers,
            initializer=_runner_initializer,
            initargs=(config.name, config.address, use_ray),
            mp_context=mp_context,
        )

    @property
    def name(self) -> str:
        return self._config.name

    @property
    def capacity(self) -> int:
        return self._config.capacity

    def submit_chunk(
        self,
        serialized_fn: bytes,
        indexed_items: List[Tuple[int, object]],
        retries: int,
        on_error: str,
    ) -> Future:
        return self._pool.submit(_execute_chunk, serialized_fn, indexed_items, retries, on_error)

    def shutdown(self) -> None:
        self._pool.shutdown(wait=False, cancel_futures=False)
