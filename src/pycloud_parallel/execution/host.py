from __future__ import annotations

"""V1 execution-host entrypoint."""

from pycloud_parallel.controlplane.executor_host import ExecutorHostClient

ExecutionHost = ExecutorHostClient

__all__ = [
    "ExecutionHost",
    "ExecutorHostClient",
]
