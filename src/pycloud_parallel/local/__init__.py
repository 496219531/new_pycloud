from __future__ import annotations

"""Stable local-only parallel entrypoint."""

from pycloud_parallel.local_runtime import ForeachResult, TaskError, configure, foreach, parallel_for

__all__ = [
    "ForeachResult",
    "TaskError",
    "configure",
    "foreach",
    "parallel_for",
]
