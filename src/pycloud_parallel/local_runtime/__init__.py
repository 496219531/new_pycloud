"""Local multiprocessing runtime implementation."""

from .api import configure, foreach, last_errors, metrics, parallel_for, project
from .config import ProjectConfig, RuntimeConfig
from .types import ForeachResult, TaskError

__all__ = [
    "ProjectConfig",
    "RuntimeConfig",
    "ForeachResult",
    "TaskError",
    "configure",
    "foreach",
    "last_errors",
    "metrics",
    "parallel_for",
    "project",
]
