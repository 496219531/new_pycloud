from __future__ import annotations

"""Future public TaskPool API facade."""

from pycloud_parallel.execution.task_pool import TaskPool

__all__ = [
    "TaskPool",
]


def __dir__() -> list[str]:
    return list(__all__)


try:
    del annotations
except NameError:
    pass
