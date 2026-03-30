from __future__ import annotations

"""中文说明：本地多进程执行 Runner。"""

import multiprocessing as mp
import os
from concurrent.futures import Future, ProcessPoolExecutor
from typing import List, Tuple

import cloudpickle

from .types import TaskError, UserFunctionError


def _execute_chunk(
    serialized_fn: bytes,
    indexed_items: List[Tuple[int, object]],
    retries: int,
    on_error: str,
) -> Tuple[List[Tuple[int, object]], List[TaskError]]:
    """本地执行分片任务。"""
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
                        cause=repr(exc),
                    ) from exc
                errors.append(
                    TaskError(
                        index=index,
                        item_repr=repr(item),
                        error=repr(exc),
                        attempts=attempt + 1,
                    )
                )
                break

    return values, errors


class ProcessClusterRunner:
    """本地进程池执行器。"""

    def __init__(self, max_workers: int) -> None:
        self._max_workers = max(1, int(max_workers))
        workers = max(1, min(self._max_workers, os.cpu_count() or self._max_workers))
        mp_context = None
        if os.name != "nt":
            try:
                mp_context = mp.get_context("fork")
            except ValueError:
                mp_context = None
        self._pool = ProcessPoolExecutor(max_workers=workers, mp_context=mp_context)

    @property
    def name(self) -> str:
        return "local"

    @property
    def capacity(self) -> int:
        return self._max_workers

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
