"""V1 public API package target."""

from .common import DataRef, export
from .pool import TaskPool
from .queue import JobQueue
from .service import Service

__all__ = [
    "DataRef",
    "JobQueue",
    "Service",
    "TaskPool",
    "export",
]


def __dir__() -> list[str]:
    return list(__all__)


try:
    del annotations
except NameError:
    pass
