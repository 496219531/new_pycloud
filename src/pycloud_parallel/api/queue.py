from __future__ import annotations

"""Future public JobQueue API facade."""

from pycloud_parallel.execution.queue import JobQueue

__all__ = [
    "JobQueue",
]


def __dir__() -> list[str]:
    return list(__all__)


try:
    del annotations
except NameError:
    pass
