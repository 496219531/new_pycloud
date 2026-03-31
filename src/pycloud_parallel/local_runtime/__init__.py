"""Local multiprocessing runtime implementation."""

from .api import configure, foreach, parallel_for
from .types import ForeachResult, TaskError

__all__ = [
    "ForeachResult",
    "TaskError",
    "configure",
    "foreach",
    "parallel_for",
]
