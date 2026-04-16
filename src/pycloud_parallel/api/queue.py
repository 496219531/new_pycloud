from __future__ import annotations

"""Public JobQueue API."""

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
