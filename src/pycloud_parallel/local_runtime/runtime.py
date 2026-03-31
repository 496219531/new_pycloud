from __future__ import annotations

"""Internal runtime state for lightweight local multiprocessing."""

from concurrent.futures import ProcessPoolExecutor
import itertools
import multiprocessing as mp
import os
import threading
from typing import Iterable, List, Optional, Sequence, Tuple, Union

import cloudpickle

from .types import ForeachResult, TaskError


_STATE_LOCK = threading.Lock()
_CONFIG = max(1, os.cpu_count() or 1)
_POOL: Optional[ProcessPoolExecutor] = None
_POOL_WORKERS = 0


def _normalize_max_workers(max_workers: Optional[int]) -> int:
    if max_workers is None:
        return max(1, os.cpu_count() or 1)
    return max(1, int(max_workers))


def _build_pool(max_workers: int) -> ProcessPoolExecutor:
    workers = max(1, min(int(max_workers), os.cpu_count() or int(max_workers)))
    try:
        mp_context = mp.get_context("spawn")
    except ValueError:
        mp_context = None
    return ProcessPoolExecutor(max_workers=workers, mp_context=mp_context)


def _shutdown_pool(pool: Optional[ProcessPoolExecutor]) -> None:
    if pool is None:
        return
    pool.shutdown(wait=False, cancel_futures=False)


def _ensure_global_pool() -> ProcessPoolExecutor:
    global _POOL, _POOL_WORKERS
    with _STATE_LOCK:
        workers = int(_CONFIG)
        if _POOL is None or _POOL_WORKERS != workers:
            old_pool = _POOL
            _POOL = _build_pool(workers)
            _POOL_WORKERS = workers
            _shutdown_pool(old_pool)
        return _POOL


def _execute_item(
    serialized_fn: bytes,
    indexed_item: Tuple[int, object],
) -> Tuple[int, bool, object]:
    fn = cloudpickle.loads(serialized_fn)
    index, item = indexed_item
    try:
        return index, True, fn(item)
    except Exception as exc:
        return index, False, TaskError(
            index=index,
            item_repr=repr(item),
            error=repr(exc),
            attempts=1,
        )


def _run_with_pool(
    *,
    pool: ProcessPoolExecutor,
    iterable: Union[Sequence[object], Iterable[object]],
    fn,
) -> ForeachResult:
    try:
        serialized_fn = cloudpickle.dumps(fn)
    except Exception as exc:
        raise RuntimeError(f"failed to serialize function for parallel execution: {exc}") from exc

    values: List[object] = []
    errors: List[TaskError] = []
    iterator = pool.map(
        _execute_item,
        itertools.repeat(serialized_fn),
        enumerate(iterable),
    )
    for _, ok, payload in iterator:
        if ok:
            values.append(payload)
        else:
            errors.append(payload)
    return ForeachResult(values=values, errors=errors)


def _configure_runtime(*, max_workers: Optional[int] = None, reset: bool = True) -> int:
    global _CONFIG, _POOL, _POOL_WORKERS
    with _STATE_LOCK:
        if reset:
            old_pool = _POOL
            _POOL = None
            _POOL_WORKERS = 0
            _CONFIG = _normalize_max_workers(max_workers)
            _shutdown_pool(old_pool)
        elif max_workers is not None:
            _CONFIG = _normalize_max_workers(max_workers)
    return _CONFIG


def _foreach(
    iterable: Union[Sequence[object], Iterable[object]],
    fn,
    *,
    max_workers: Optional[int],
) -> ForeachResult:
    if max_workers is not None and max_workers > 0:
        pool = _build_pool(int(max_workers))
        try:
            return _run_with_pool(pool=pool, iterable=iterable, fn=fn)
        finally:
            _shutdown_pool(pool)

    return _run_with_pool(pool=_ensure_global_pool(), iterable=iterable, fn=fn)
