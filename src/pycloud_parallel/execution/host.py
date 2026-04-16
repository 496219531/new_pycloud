from __future__ import annotations

"""Execution host facade for the V1 execution package."""

from pycloud_parallel.controlplane.executor_host import ExecutorHostClient

ExecutionHost = ExecutorHostClient

__all__ = [
    "ExecutionHost",
    "ExecutorHostClient",
]
