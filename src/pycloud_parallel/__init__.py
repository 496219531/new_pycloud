"""Public API for lightweight local parallel execution."""

from .local_runtime.api import configure, foreach, parallel_for
from .local_runtime.types import ForeachResult, TaskError

__all__ = [
    "ForeachResult",
    "TaskError",
    "configure",
    "foreach",
    "parallel_for",
]
