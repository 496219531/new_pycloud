from __future__ import annotations

"""Public TaskPool API."""

from pycloud_parallel.execution.task_pool import TaskPool

__all__ = [
    "TaskPool",
]


def __dir__() -> list[str]:
    return list(__all__)
